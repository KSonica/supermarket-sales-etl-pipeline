import pandas as pd
import requests
import logging
from io import StringIO
from config.config import DATA_SOURCE_URL

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_data(source_url: str = DATA_SOURCE_URL) -> pd.DataFrame:
    """
    Extract supermarket sales data from the source CSV URL.

    Args:
        source_url: URL of the CSV data source

    Returns:
        DataFrame with raw supermarket sales data
    """
    logger.info(f'Extracting data from: {source_url}')

    try:
        response = requests.get(source_url, timeout=30)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        logger.info(f'Successfully extracted {len(df)} rows and {len(df.columns)} columns')
        return df

    except requests.exceptions.HTTPError as e:
        logger.error(f'HTTP error during extraction: {e}')
        raise
    except Exception as e:
        logger.error(f'Unexpected error during extraction: {e}')
        raise


def validate_raw_data(df: pd.DataFrame) -> bool:
    """
    Validate the extracted raw data has expected columns and structure.

    Args:
        df: Raw DataFrame to validate

    Returns:
        True if data is valid, raises ValueError otherwise
    """
    expected_columns = [
        'Invoice ID', 'Branch', 'City', 'Customer type', 'Gender',
        'Product line', 'Unit price', 'Quantity', 'Tax 5%', 'Total',
        'Date', 'Time', 'Payment', 'cogs', 'gross margin percentage',
        'gross income', 'Rating'
    ]

    missing_cols = [col for col in expected_columns if col not in df.columns]

    if missing_cols:
        raise ValueError(f'Missing expected columns: {missing_cols}')

    if df.empty:
        raise ValueError('Extracted DataFrame is empty')

    logger.info('Raw data validation passed')
    return True
