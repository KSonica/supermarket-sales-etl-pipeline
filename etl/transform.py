import pandas as pd
import numpy as np
import logging
import hashlib
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def generate_surrogate_key(*args) -> str:
    """Generate a deterministic surrogate key from one or more values."""
    combined = '_'.join(str(a) for a in args)
    return hashlib.md5(combined.encode()).hexdigest()[:16]


def transform_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw data into the dim_product dimension table.
    
    Columns: product_id, product_line, unit_price
    """
    logger.info('Transforming dim_product...')
    
    dim_product = df[['Product line', 'Unit price']].drop_duplicates().copy()
    dim_product.columns = ['product_line', 'unit_price']
    dim_product['product_id'] = dim_product.apply(
        lambda row: generate_surrogate_key(row['product_line'], row['unit_price']), axis=1
    )
    dim_product = dim_product[['product_id', 'product_line', 'unit_price']]
    
    logger.info(f'dim_product: {len(dim_product)} rows')
    return dim_product


def transform_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw data into the dim_customer dimension table.
    
    Columns: customer_id, customer_type, gender
    """
    logger.info('Transforming dim_customer...')
    
    dim_customer = df[['Customer type', 'Gender']].drop_duplicates().copy()
    dim_customer.columns = ['customer_type', 'gender']
    dim_customer['customer_id'] = dim_customer.apply(
        lambda row: generate_surrogate_key(row['customer_type'], row['gender']), axis=1
    )
    dim_customer = dim_customer[['customer_id', 'customer_type', 'gender']]
    
    logger.info(f'dim_customer: {len(dim_customer)} rows')
    return dim_customer


def transform_dim_branch(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw data into the dim_branch dimension table.
    
    Columns: branch_id, branch, city
    """
    logger.info('Transforming dim_branch...')
    
    dim_branch = df[['Branch', 'City']].drop_duplicates().copy()
    dim_branch.columns = ['branch', 'city']
    dim_branch['branch_id'] = dim_branch.apply(
        lambda row: generate_surrogate_key(row['branch'], row['city']), axis=1
    )
    dim_branch = dim_branch[['branch_id', 'branch', 'city']]
    
    logger.info(f'dim_branch: {len(dim_branch)} rows')
    return dim_branch


def transform_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw data into the dim_date dimension table.
    
    Columns: date_id, date, year, month, day, quarter, day_of_week, hour
    """
    logger.info('Transforming dim_date...')
    
    df['_datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    date_df = df[['_datetime']].drop_duplicates().copy()
    
    dim_date = pd.DataFrame()
    dim_date['date'] = date_df['_datetime'].dt.date
    dim_date['year'] = date_df['_datetime'].dt.year
    dim_date['month'] = date_df['_datetime'].dt.month
    dim_date['day'] = date_df['_datetime'].dt.day
    dim_date['quarter'] = date_df['_datetime'].dt.quarter
    dim_date['day_of_week'] = date_df['_datetime'].dt.dayofweek
    dim_date['hour'] = date_df['_datetime'].dt.hour
    dim_date['date_id'] = date_df['_datetime'].apply(
        lambda dt: generate_surrogate_key(dt.strftime('%Y-%m-%d %H:%M'))
    )
    dim_date = dim_date[['date_id', 'date', 'year', 'month', 'day', 'quarter', 'day_of_week', 'hour']]
    
    logger.info(f'dim_date: {len(dim_date)} rows')
    return dim_date


def transform_dim_payment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw data into the dim_payment dimension table.
    
    Columns: payment_id, payment_method
    """
    logger.info('Transforming dim_payment...')
    
    dim_payment = df[['Payment']].drop_duplicates().copy()
    dim_payment.columns = ['payment_method']
    dim_payment['payment_id'] = dim_payment['payment_method'].apply(
        lambda x: generate_surrogate_key(x)
    )
    dim_payment = dim_payment[['payment_id', 'payment_method']]
    
    logger.info(f'dim_payment: {len(dim_payment)} rows')
    return dim_payment


def transform_fact_sales(
    df: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_branch: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_payment: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform raw data into the fact_sales fact table by joining dimension keys.
    
    Columns: invoice_id, product_id, customer_id, branch_id, date_id,
             payment_id, quantity, unit_price, tax_5_percent, total,
             cogs, gross_margin_pct, gross_income, rating
    """
    logger.info('Transforming fact_sales...')
    
    df = df.copy()
    df['_datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    
    # Merge product dimension
    df = df.merge(
        dim_product[['product_id', 'product_line', 'unit_price']],
        left_on=['Product line', 'Unit price'],
        right_on=['product_line', 'unit_price'],
        how='left'
    )
    
    # Merge customer dimension
    df = df.merge(
        dim_customer[['customer_id', 'customer_type', 'gender']],
        left_on=['Customer type', 'Gender'],
        right_on=['customer_type', 'gender'],
        how='left'
    )
    
    # Merge branch dimension
    df = df.merge(
        dim_branch[['branch_id', 'branch', 'city']],
        left_on=['Branch', 'City'],
        right_on=['branch', 'city'],
        how='left'
    )
    
    # Merge date dimension
    dim_date_lookup = dim_date.copy()
    df['_date_key'] = df['_datetime'].apply(
        lambda dt: generate_surrogate_key(dt.strftime('%Y-%m-%d %H:%M'))
    )
    df = df.merge(
        dim_date[['date_id']],
        left_on='_date_key',
        right_on='date_id',
        how='left'
    )
    
    # Merge payment dimension
    df = df.merge(
        dim_payment[['payment_id', 'payment_method']],
        left_on='Payment',
        right_on='payment_method',
        how='left'
    )
    
    fact_sales = df[[
        'Invoice ID', 'product_id', 'customer_id', 'branch_id', 'date_id',
        'payment_id', 'Quantity', 'Unit price', 'Tax 5%', 'Total',
        'cogs', 'gross margin percentage', 'gross income', 'Rating'
    ]].copy()
    
    fact_sales.columns = [
        'invoice_id', 'product_id', 'customer_id', 'branch_id', 'date_id',
        'payment_id', 'quantity', 'unit_price', 'tax_5_percent', 'total',
        'cogs', 'gross_margin_pct', 'gross_income', 'rating'
    ]
    
    logger.info(f'fact_sales: {len(fact_sales)} rows')
    return fact_sales


def transform_all(df: pd.DataFrame) -> dict:
    """
    Run all transformations and return a dict of DataFrames for each table.
    
    Returns:
        dict with keys: dim_product, dim_customer, dim_branch, dim_date,
                        dim_payment, fact_sales
    """
    logger.info('Starting full transformation pipeline...')
    
    dim_product = transform_dim_product(df)
    dim_customer = transform_dim_customer(df)
    dim_branch = transform_dim_branch(df)
    dim_date = transform_dim_date(df)
    dim_payment = transform_dim_payment(df)
    fact_sales = transform_fact_sales(
        df, dim_product, dim_customer, dim_branch, dim_date, dim_payment
    )
    
    logger.info('Transformation pipeline complete')
    
    return {
        'dim_product': dim_product,
        'dim_customer': dim_customer,
        'dim_branch': dim_branch,
        'dim_date': dim_date,
        'dim_payment': dim_payment,
        'fact_sales': fact_sales
    }
