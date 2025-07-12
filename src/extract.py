import re
import pandas as pd
import logging
from typing import List, Dict, Tuple, Any

logger = logging.getLogger(__name__)


def list_s3_files(s3_client: Any, bucket_name: str, prefix: str) -> List[str]:
    """
    List all CSV files in an S3 bucket under a given prefix.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
    except Exception as e:
        logger.error(f"Error listing S3 files: {e}")
        raise


def is_valid_csv_filename(file_name: str, directory_name: str, prefix: str) -> bool:
    """
    Checks if the file_name matches the pattern: directory_name/<prefix><three-digit-number>.csv
    """
    pattern = rf"^{re.escape(directory_name)}/{re.escape(prefix)}\d{{3}}\.csv$"
    return bool(re.match(pattern, file_name))


def read_csv_file(s3_client: Any, bucket_name: str, key: str) -> pd.DataFrame:
    """
    Reads a CSV file from S3 and returns a pandas DataFrame.
    """
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(obj['Body'])
        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"File at {bucket_name}/{key} did not return a DataFrame.")
        return df
    except Exception as e:
        logger.error(f"Error reading CSV from S3: {e}")
        raise


def validate_csv_schema_by_production_code(
    df: pd.DataFrame, expected_schemas: Dict[str, List[str]]
) -> Tuple[bool, Any]:
    """
    Validates the CSV header based on the schema inferred from the 'Production Code' column value.
    Returns (True, message_or_df) or (False, error_message).
    """
    try:
        actual_headers = list(df.columns)
        if "Production Code" not in actual_headers:
            return False, "Missing required column: 'Production Code'"
        prod_code_series = df["Production Code"].dropna()
        if prod_code_series.empty:
            return False, "'Production Code' column is empty"
        production_code_value = str(prod_code_series.iloc[0])
        prefix = production_code_value[:2]
        if prefix not in expected_schemas:
            return False, f"No schema defined for Production Code prefix '{prefix}'"
        expected_headers = expected_schemas[prefix]
        if actual_headers == expected_headers:
            return True, f"Headers match expected schema for prefix '{prefix}'."
        if len(actual_headers) == len(expected_headers):
            df.columns = expected_headers
            return True, df
        return False, (
            f"Header mismatch for prefix '{prefix}'.\n"
            f"Expected: {expected_headers}\n"
            f"Found:    {actual_headers}"
        )
    except Exception as e:
        logger.error(f"Error validating CSV schema: {e}")
        return False, f"Error processing file: {e}"


def drop_invalid_production_codes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes records where 'Production Code' does not match the expected pattern: AA999.
    """
    if "Production Code" not in df.columns:
        logger.error("Missing required column: 'Production Code'")
        raise ValueError("Missing required column: 'Production Code'")
    valid_pattern = r"^[A-Z]{2}\d{3}$"
    filtered_df = df[df["Production Code"].astype(str).str.match(valid_pattern, na=False)].copy()
    return filtered_df