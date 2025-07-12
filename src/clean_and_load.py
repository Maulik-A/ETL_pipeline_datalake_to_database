import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from .config import EXPECTED_SCHEMAS, DB_URI
import logging
from .extract import *
from typing import Any

logger = logging.getLogger(__name__)

def cleaning_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicates, drop rows with any nulls, and add processing timestamp.
    """
    df = df.drop_duplicates()
    df = df.dropna()
    df['processing_ts'] = datetime.now()
    return df

def load_to_postgres(df: pd.DataFrame, table_name: str) -> None:
    """
    Load DataFrame to Postgres table.
    """
    try:
        engine = create_engine(DB_URI)
        df.to_sql(table_name, engine, if_exists='append', index=False)
    except Exception as e:
        logger.error(f"Error loading data to Postgres: {e}")
        raise

def process_file(s3_client: Any, bucket_name: str, prefix: str) -> None:
    """
    Process all CSV files from S3 and load valid data to Postgres.
    """
    for item in list_s3_files(s3_client=s3_client, bucket_name=bucket_name, prefix=prefix):
        try:
            df = read_csv_file(s3_client, bucket_name, item)
            valid, res = validate_csv_schema_by_production_code(df, EXPECTED_SCHEMAS)
            if valid:
                if isinstance(res, pd.DataFrame):
                    df = res  # Use standardized header if returned
                try:
                    df_clean = drop_invalid_production_codes(df=df)
                except Exception as e:
                    logger.error(f"Error while dropping invalid production code: {e}")
                    continue
                if df_clean is None or df_clean.empty:
                    logger.info(f"No valid records found in the dataframe: {item}")
                    continue
                df_clean = cleaning_data(df_clean)
                table_name = df_clean["Production Code"].iloc[0]
                table_name = f"stg_{table_name}"
                try:
                    load_to_postgres(df_clean, table_name)
                    logger.info(f"Data ingested successfully {table_name}")
                except Exception as e:
                    logger.error(f"Error while loading data in postgres: {e}")
            else:
                logger.warning(f"Schema validation failed for {item}: {res}")
        except Exception as e:
            logger.error(f"Failed to process file {item}: {e}")