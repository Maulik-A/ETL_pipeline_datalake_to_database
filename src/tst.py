import boto3
import os
import pandas as pd
from sqlalchemy import create_engine
import re
from config import EXPECTED_SCHEMAS, DB_URI
from datetime import datetime

s3 = boto3.client('s3',
                  endpoint_url = os.environ.get("endpoint_url"),
                  aws_access_key_id=os.environ.get("aws_access_key_id"),
                  aws_secret_access_key=os.environ.get("aws_secret_access_key"))

# obj = s3.get_object(Bucket='mybucket', Key='raw/CSV 2.csv')

# df = pd.read_csv(obj['Body'])

# print(df.head())

# engine = sqlalchemy.create_engine(os.environ.get("db_uri"))
# df.to_sql(con=engine,
#               name='temporary_table',
#               if_exists='append',
#               index=False,
#               method='multi')


def list_s3_files(s3_client, prefix):
    response = s3_client.list_objects_v2(Bucket='mybucket', Prefix=prefix)
    return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

# print(list_s3_files(s3, 'raw/'))



def is_valid_csv_filename(file_name: str, directory_name: str, prefix: str) -> bool:
    """
    Checks if the file_name matches the pattern: directory_name/<prefix><three-digit-number>.csv

    Parameters:
    - file_name (str): Full file path in S3 (e.g., 'my-directory/AB001.csv')
    - directory_name (str): Expected directory name in S3 (e.g., 'my-directory')
    - prefix (str): Expected fixed prefix before the number (e.g., 'AB')

    Returns:
    - bool: True if the filename matches the pattern, False otherwise
    """

    # Define regex pattern
    pattern = rf"^{re.escape(directory_name)}/{re.escape(prefix)}\d{{3}}\.csv$"

    return bool(re.match(pattern, file_name))


def read_csv_file(s3_client:object, bucket_name:str, key:str)-> pd.DataFrame:
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(obj['Body'])
    if isinstance(df, pd.DataFrame):
        return df
    else:
        raise f'''Error reading file at this location: {bucket_name}/{key}'''
# if len(production_code) == 1:
#     expected_file_name = prefix+production_code[0]+".csv"
# elif len(production_code) == 0:
#     print("Missing production code")
# else:
#     print("Multiple production code found")

def validate_csv_schema_by_production_code(df:pd.DataFrame, expected_schemas: dict) -> tuple[bool, str]:
    """
    Validates the CSV header based on the schema inferred from the 'Production Code' column value.

    Parameters:
    - df (dataframe): Pandas dataframe from CSV file
    - expected_schemas (dict): Dictionary of expected schemas keyed by Production Code prefix (e.g., 'AB', 'CD')

    Returns:
    - (bool, str): Tuple with validation result and message
    """

    try:

        # Validate header exists
        actual_headers = list(df.columns)
        if "Production Code" not in actual_headers:
            return False, "Missing required column: 'Production Code'"

        # Get first non-null Production Code
        prod_code_series = df["Production Code"].dropna()
        if prod_code_series.empty:
            return False, "'Production Code' column is empty"

        production_code_value = prod_code_series.iloc[0]
        prefix = str(production_code_value)[:2]  # First two letters as schema key

        if prefix not in expected_schemas:
            return False, f"No schema defined for Production Code prefix '{prefix}'"

        expected_headers = expected_schemas[prefix]

        if actual_headers == expected_headers:
            return True, f"Headers match expected schema for prefix '{prefix}'."
        
        if len(actual_headers) == len(expected_headers):
            df.columns = expected_headers  # Replace with standard header
            return True, df
        else:
            return False, (
                f"Header mismatch for prefix '{prefix}'.\n"
                f"Expected: {expected_headers}\n"
                f"Found:    {actual_headers}"
            )

    except Exception as e:
        return False, f"Error processing file: {e}"

def drop_invalid_production_codes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes records where 'Production Code' does not match the expected pattern: AA999.

    Parameters:
    - df (pd.DataFrame): Input DataFrame

    Returns:
    - pd.DataFrame: Filtered DataFrame with only valid 'Production Code' entries
    """

    if "Production Code" not in df.columns:
        raise ValueError("Missing required column: 'Production Code'")

    # Regex pattern for valid Production Code
    valid_pattern = r"^[A-Z]{2}\d{3}$"

    # Filter rows where Production Code matches the pattern
    filtered_df = df[df["Production Code"]
                     .astype(str)
                     .str.match(valid_pattern, na=False)].copy()

    return filtered_df

def cleanning_data(df:pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    df = df.dropna()
    df['processing_ts'] = datetime.now()
    return df

def load_to_postgres(df, table_name):
    engine = create_engine(DB_URI)
    df.to_sql(table_name, engine, if_exists='append', index=False)


def main(s3_client: object, bucket_name:str, prefix: str):
    for items in list_s3_files(s3_client, prefix):
        df = read_csv_file(s3_client, bucket_name, items)

        valid, res = validate_csv_schema_by_production_code(df, EXPECTED_SCHEMAS)
        if valid:
            try:
                df_clean = drop_invalid_production_codes(df=df)
            except Exception as e:
                raise f"Error while dropping invalid produciton code: {e}"
            
            if df_clean is None or df_clean.empty:
                print(f"No valid records found in the dataframe: {items}")
            else:
                df_clean = cleanning_data(df)
                table_name = df_clean["Production Code"].iloc[0]
                table_name = "stg_"+table_name
            
            try:
                load_to_postgres(df_clean, table_name)
                print(f"Data ingested successfully {table_name}")
            except:
                raise "Error while loading data in postgres"
        else:
            print(res)

if __name__ == "__main__":
    main(s3_client=s3, bucket_name='mybucket' ,prefix='raw/')




