-- ============================================================
-- Supermarket Sales ETL Pipeline - BigQuery Schema
-- Star Schema Design with Fact and Dimension Tables
-- ============================================================

-- Dimension Table: dim_product
-- Captures product line and category information
CREATE TABLE IF NOT EXISTS `etl-pipeline-492723.supermarket_sales.dim_product` (
    product_id      STRING NOT NULL,
    product_line    STRING NOT NULL,
    unit_price      FLOAT64 NOT NULL
)
OPTIONS (
    description = 'Product dimension table containing product line and pricing info'
);

-- Dimension Table: dim_customer
-- Captures customer type and gender demographics
CREATE TABLE IF NOT EXISTS `etl-pipeline-492723.supermarket_sales.dim_customer` (
    customer_id     STRING NOT NULL,
    customer_type   STRING NOT NULL,
    gender          STRING NOT NULL
)
OPTIONS (
    description = 'Customer dimension table containing customer demographics'
);

-- Dimension Table: dim_branch
-- Captures branch and city location information
CREATE TABLE IF NOT EXISTS `etl-pipeline-492723.supermarket_sales.dim_branch` (
    branch_id   STRING NOT NULL,
    branch      STRING NOT NULL,
    city        STRING NOT NULL
)
OPTIONS (
    description = 'Branch dimension table containing store location details'
);

-- Dimension Table: dim_date
-- Captures date and time breakdown for time-series analysis
CREATE TABLE IF NOT EXISTS `etl-pipeline-492723.supermarket_sales.dim_date` (
    date_id     STRING NOT NULL,
    date        DATE NOT NULL,
    year        INT64 NOT NULL,
    month       INT64 NOT NULL,
    day         INT64 NOT NULL,
    quarter     INT64 NOT NULL,
    day_of_week INT64 NOT NULL,
    hour        INT64 NOT NULL
)
OPTIONS (
    description = 'Date dimension table for time-series analysis'
);

-- Dimension Table: dim_payment
-- Captures payment method information
CREATE TABLE IF NOT EXISTS `etl-pipeline-492723.supermarket_sales.dim_payment` (
    payment_id      STRING NOT NULL,
    payment_method  STRING NOT NULL
)
OPTIONS (
    description = 'Payment dimension table containing payment method details'
);

-- Fact Table: fact_sales
-- Central fact table linking all dimensions with measurable metrics
CREATE TABLE IF NOT EXISTS `etl-pipeline-492723.supermarket_sales.fact_sales` (
    invoice_id      STRING NOT NULL,
    product_id      STRING NOT NULL,
    customer_id     STRING NOT NULL,
    branch_id       STRING NOT NULL,
    date_id         STRING NOT NULL,
    payment_id      STRING NOT NULL,
    quantity        INT64 NOT NULL,
    unit_price      FLOAT64 NOT NULL,
    tax_5_percent   FLOAT64 NOT NULL,
    total           FLOAT64 NOT NULL,
    cogs            FLOAT64 NOT NULL,
    gross_margin_pct FLOAT64 NOT NULL,
    gross_income    FLOAT64 NOT NULL,
    rating          FLOAT64 NOT NULL
)
OPTIONS (
    description = 'Fact table containing all sales transactions with foreign keys to dimensions'
);
