import boto3
import os
import logging
from src.clean_and_load import process_file

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
    )

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=os.environ.get("endpoint_url"),
            aws_access_key_id=os.environ.get("aws_access_key_id"),
            aws_secret_access_key=os.environ.get("aws_secret_access_key")
        )
        logger.info("Starting ETL pipeline...")
        process_file(s3_client=s3, bucket_name='mybucket', prefix='raw/')
        logger.info("ETL pipeline completed successfully.")
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")

if __name__ == "__main__":
    main()
