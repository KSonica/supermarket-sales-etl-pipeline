import pandas as pd
import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from config.config import (
    PROJECT_ID, DATASET_ID, LOCATION,
    DIM_PRODUCT_TABLE_ID, DIM_CUSTOMER_TABLE_ID,
    DIM_BRANCH_TABLE_ID, DIM_DATE_TABLE_ID,
    DIM_PAYMENT_TABLE_ID, FACT_SALES_TABLE_ID
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_bigquery_client() -> bigquery.Client:
    """Create and return a BigQuery client."""
    return bigquery.Client(project=PROJECT_ID)


def create_dataset_if_not_exists(client: bigquery.Client) -> None:
    """
    Create the BigQuery dataset if it does not already exist.
    
    Args:
        client: BigQuery client
    """
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
        logger.info(f'Dataset {DATASET_ID} already exists')
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = LOCATION
        dataset.description = 'Supermarket Sales ETL Pipeline - Star Schema Data Warehouse'
        client.create_dataset(dataset)
        logger.info(f'Dataset {DATASET_ID} created in {LOCATION}')


def load_dataframe_to_bigquery(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_id: str,
    write_disposition: str = 'WRITE_TRUNCATE'
) -> None:
    """
    Load a pandas DataFrame to a BigQuery table.
    
    Args:
        client: BigQuery client
        df: DataFrame to load
        table_id: Full BigQuery table ID (project.dataset.table)
        write_disposition: How to handle existing data (WRITE_TRUNCATE or WRITE_APPEND)
    """
    logger.info(f'Loading {len(df)} rows to {table_id}...')
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    
    table = client.get_table(table_id)
    logger.info(f'Loaded {table.num_rows} rows into {table_id}')


def load_all_tables(transformed_data: dict) -> None:
    """
    Load all transformed dimension and fact tables into BigQuery.
    
    Args:
        transformed_data: Dict of {table_name: DataFrame} from transform_all()
    """
    logger.info('Initializing BigQuery client and dataset...')
    client = get_bigquery_client()
    create_dataset_if_not_exists(client)
    
    table_mapping = {
        'dim_product': DIM_PRODUCT_TABLE_ID,
        'dim_customer': DIM_CUSTOMER_TABLE_ID,
        'dim_branch': DIM_BRANCH_TABLE_ID,
        'dim_date': DIM_DATE_TABLE_ID,
        'dim_payment': DIM_PAYMENT_TABLE_ID,
        'fact_sales': FACT_SALES_TABLE_ID
    }
    
    # Load dimension tables first, then fact table
    dim_tables = ['dim_product', 'dim_customer', 'dim_branch', 'dim_date', 'dim_payment']
    
    for table_name in dim_tables:
        df = transformed_data[table_name]
        table_id = table_mapping[table_name]
        load_dataframe_to_bigquery(client, df, table_id)
    
    # Load fact table last
    load_dataframe_to_bigquery(client, transformed_data['fact_sales'], FACT_SALES_TABLE_ID)
    
    logger.info('All tables loaded to BigQuery successfully')


def verify_load(table_id: str) -> int:
    """
    Verify data was loaded successfully by counting rows.
    
    Args:
        table_id: Full BigQuery table ID to verify
        
    Returns:
        Number of rows in the table
    """
    client = get_bigquery_client()
    query = f'SELECT COUNT(*) as row_count FROM `{table_id}`'
    result = client.query(query).result()
    row_count = list(result)[0]['row_count']
    logger.info(f'{table_id}: {row_count} rows verified')
    return row_count
