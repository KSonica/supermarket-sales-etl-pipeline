import os
from dotenv import load_dotenv

load_dotenv()

# Google Cloud Configuration
PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'etl-pipeline-492723')
DATASET_ID = os.getenv('BQ_DATASET_ID', 'supermarket_sales')
LOCATION = os.getenv('BQ_LOCATION', 'US')

# Source Data Configuration
DATA_SOURCE_URL = os.getenv(
    'DATA_SOURCE_URL',
    'https://raw.githubusercontent.com/dsrscientist/dataset1/master/supermarket_sales.csv'
)

# Table Names
FACT_TABLE = 'fact_sales'
DIM_PRODUCT_TABLE = 'dim_product'
DIM_CUSTOMER_TABLE = 'dim_customer'
DIM_BRANCH_TABLE = 'dim_branch'
DIM_DATE_TABLE = 'dim_date'
DIM_PAYMENT_TABLE = 'dim_payment'

# BigQuery Table Full References
def get_full_table_id(table_name):
    return f'{PROJECT_ID}.{DATASET_ID}.{table_name}'

FACT_SALES_TABLE_ID = get_full_table_id(FACT_TABLE)
DIM_PRODUCT_TABLE_ID = get_full_table_id(DIM_PRODUCT_TABLE)
DIM_CUSTOMER_TABLE_ID = get_full_table_id(DIM_CUSTOMER_TABLE)
DIM_BRANCH_TABLE_ID = get_full_table_id(DIM_BRANCH_TABLE)
DIM_DATE_TABLE_ID = get_full_table_id(DIM_DATE_TABLE)
DIM_PAYMENT_TABLE_ID = get_full_table_id(DIM_PAYMENT_TABLE)
