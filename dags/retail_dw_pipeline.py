import csv
import os
from datetime import timedelta

import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


RAW_DIR = "/opt/airflow/data/raw"
OUTPUT_DIR = "/opt/airflow/data/output"


DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "retail_dw",
    "user": "airflow",
    "password": "airflow",
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def run_sql(sql: str, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


def fetch_one(sql: str, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]


def get_run_id(context):
    return context["dag_run"].run_id


def log_quality_check(run_id, check_name, layer_name, passed, actual_value, expected_rule):
    sql = """
    INSERT INTO audit.quality_checks (
        pipeline_run_id,
        check_name,
        layer_name,
        passed,
        actual_value,
        expected_rule,
        checked_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, NOW());
    """
    run_sql(sql, (run_id, check_name, layer_name, passed, str(actual_value), expected_rule))


def create_database_objects(**context):
    sql = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS warehouse;
    CREATE SCHEMA IF NOT EXISTS mart;
    CREATE SCHEMA IF NOT EXISTS audit;

    CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
        pipeline_run_id TEXT PRIMARY KEY,
        dag_id TEXT,
        status TEXT,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        notes TEXT
    );

    CREATE TABLE IF NOT EXISTS audit.quality_checks (
        quality_check_id SERIAL PRIMARY KEY,
        pipeline_run_id TEXT,
        check_name TEXT,
        layer_name TEXT,
        passed BOOLEAN,
        actual_value TEXT,
        expected_rule TEXT,
        checked_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS raw.customers_raw (
        customer_id TEXT,
        customer_name TEXT,
        country TEXT,
        signup_date TEXT
    );

    CREATE TABLE IF NOT EXISTS raw.products_raw (
        product_id TEXT,
        product_name TEXT,
        category TEXT,
        price TEXT
    );

    CREATE TABLE IF NOT EXISTS raw.orders_raw (
        order_id TEXT,
        customer_id TEXT,
        product_id TEXT,
        quantity TEXT,
        order_date TEXT,
        status TEXT
    );

    CREATE TABLE IF NOT EXISTS raw.payments_raw (
        payment_id TEXT,
        order_id TEXT,
        payment_method TEXT,
        payment_status TEXT,
        payment_date TEXT,
        amount TEXT
    );

    CREATE TABLE IF NOT EXISTS staging.customers (
        customer_id TEXT PRIMARY KEY,
        customer_name TEXT NOT NULL,
        country TEXT NOT NULL,
        signup_date DATE
    );

    CREATE TABLE IF NOT EXISTS staging.products (
        product_id TEXT PRIMARY KEY,
        product_name TEXT NOT NULL,
        category TEXT NOT NULL,
        price NUMERIC(10, 2) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS staging.orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        product_id TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        order_date DATE NOT NULL,
        status TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS staging.payments (
        payment_id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        payment_method TEXT,
        payment_status TEXT,
        payment_date DATE,
        amount NUMERIC(10, 2)
    );

    CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
        customer_key SERIAL PRIMARY KEY,
        customer_id TEXT UNIQUE NOT NULL,
        customer_name TEXT NOT NULL,
        country TEXT NOT NULL,
        signup_date DATE
    );

    CREATE TABLE IF NOT EXISTS warehouse.dim_products (
        product_key SERIAL PRIMARY KEY,
        product_id TEXT UNIQUE NOT NULL,
        product_name TEXT NOT NULL,
        category TEXT NOT NULL,
        price NUMERIC(10, 2) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS warehouse.dim_date (
        date_key INTEGER PRIMARY KEY,
        full_date DATE UNIQUE NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        month_name TEXT NOT NULL,
        quarter INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
        sales_key SERIAL PRIMARY KEY,
        order_id TEXT UNIQUE NOT NULL,
        customer_key INTEGER NOT NULL REFERENCES warehouse.dim_customers(customer_key),
        product_key INTEGER NOT NULL REFERENCES warehouse.dim_products(product_key),
        date_key INTEGER NOT NULL REFERENCES warehouse.dim_date(date_key),
        payment_method TEXT,
        quantity INTEGER NOT NULL,
        unit_price NUMERIC(10, 2) NOT NULL,
        gross_amount NUMERIC(12, 2) NOT NULL,
        payment_amount NUMERIC(12, 2),
        order_status TEXT NOT NULL,
        payment_status TEXT
    );

    CREATE TABLE IF NOT EXISTS mart.sales_by_country (
        country TEXT,
        total_orders INTEGER,
        total_quantity INTEGER,
        total_revenue NUMERIC(12, 2)
    );

    CREATE TABLE IF NOT EXISTS mart.sales_by_category (
        category TEXT,
        total_orders INTEGER,
        total_quantity INTEGER,
        total_revenue NUMERIC(12, 2)
    );

    CREATE TABLE IF NOT EXISTS mart.monthly_revenue (
        year INTEGER,
        month INTEGER,
        total_orders INTEGER,
        total_revenue NUMERIC(12, 2)
    );

    CREATE TABLE IF NOT EXISTS mart.customer_revenue (
        customer_id TEXT,
        customer_name TEXT,
        country TEXT,
        total_orders INTEGER,
        total_revenue NUMERIC(12, 2)
    );
    """
    run_sql(sql)


def start_pipeline(**context):
    run_id = get_run_id(context)
    dag_id = context["dag"].dag_id

    sql = """
    TRUNCATE TABLE
        raw.customers_raw,
        raw.products_raw,
        raw.orders_raw,
        raw.payments_raw,
        staging.customers,
        staging.products,
        staging.orders,
        staging.payments,
        warehouse.fact_sales,
        warehouse.dim_customers,
        warehouse.dim_products,
        warehouse.dim_date,
        mart.sales_by_country,
        mart.sales_by_category,
        mart.monthly_revenue,
        mart.customer_revenue
    RESTART IDENTITY CASCADE;

    INSERT INTO audit.pipeline_runs (
        pipeline_run_id,
        dag_id,
        status,
        started_at,
        notes
    )
    VALUES (%s, %s, 'running', NOW(), 'Pipeline started successfully.')
    ON CONFLICT (pipeline_run_id)
    DO UPDATE SET
        status = EXCLUDED.status,
        started_at = EXCLUDED.started_at,
        finished_at = NULL,
        notes = EXCLUDED.notes;
    """
    run_sql(sql, (run_id, dag_id))


def load_csv_to_raw_table(filename, table_name, columns):
    path = os.path.join(RAW_DIR, filename)

    with open(path, newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        rows = [[row[col] for col in columns] for row in reader]

    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s;"

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows)


def extract_raw_csv_files(**context):
    load_csv_to_raw_table(
        "customers.csv",
        "raw.customers_raw",
        ["customer_id", "customer_name", "country", "signup_date"],
    )
    load_csv_to_raw_table(
        "products.csv",
        "raw.products_raw",
        ["product_id", "product_name", "category", "price"],
    )
    load_csv_to_raw_table(
        "orders.csv",
        "raw.orders_raw",
        ["order_id", "customer_id", "product_id", "quantity", "order_date", "status"],
    )
    load_csv_to_raw_table(
        "payments.csv",
        "raw.payments_raw",
        ["payment_id", "order_id", "payment_method", "payment_status", "payment_date", "amount"],
    )


def clean_and_load_staging(**context):
    sql = """
    INSERT INTO staging.customers (
        customer_id,
        customer_name,
        country,
        signup_date
    )
    SELECT
        TRIM(customer_id),
        INITCAP(TRIM(customer_name)),
        INITCAP(TRIM(country)),
        signup_date::DATE
    FROM raw.customers_raw
    WHERE NULLIF(TRIM(customer_id), '') IS NOT NULL;

    INSERT INTO staging.products (
        product_id,
        product_name,
        category,
        price
    )
    SELECT
        TRIM(product_id),
        INITCAP(TRIM(product_name)),
        INITCAP(TRIM(category)),
        price::NUMERIC(10, 2)
    FROM raw.products_raw
    WHERE NULLIF(TRIM(product_id), '') IS NOT NULL
      AND price::NUMERIC(10, 2) > 0;

    INSERT INTO staging.orders (
        order_id,
        customer_id,
        product_id,
        quantity,
        order_date,
        status
    )
    SELECT
        TRIM(order_id),
        TRIM(customer_id),
        TRIM(product_id),
        quantity::INTEGER,
        order_date::DATE,
        LOWER(TRIM(status))
    FROM raw.orders_raw
    WHERE NULLIF(TRIM(order_id), '') IS NOT NULL
      AND quantity::INTEGER > 0;

    INSERT INTO staging.payments (
        payment_id,
        order_id,
        payment_method,
        payment_status,
        payment_date,
        amount
    )
    SELECT
        TRIM(payment_id),
        TRIM(order_id),
        INITCAP(TRIM(payment_method)),
        LOWER(TRIM(payment_status)),
        payment_date::DATE,
        amount::NUMERIC(10, 2)
    FROM raw.payments_raw
    WHERE NULLIF(TRIM(payment_id), '') IS NOT NULL
      AND amount::NUMERIC(10, 2) >= 0;
    """
    run_sql(sql)


def run_staging_quality_checks(**context):
    run_id = get_run_id(context)

    checks = [
        (
            "staging_customers_not_empty",
            "staging",
            fetch_one("SELECT COUNT(*) FROM staging.customers;"),
            lambda value: value > 0,
            "staging.customers count must be greater than 0",
        ),
        (
            "staging_products_not_empty",
            "staging",
            fetch_one("SELECT COUNT(*) FROM staging.products;"),
            lambda value: value > 0,
            "staging.products count must be greater than 0",
        ),
        (
            "staging_orders_not_empty",
            "staging",
            fetch_one("SELECT COUNT(*) FROM staging.orders;"),
            lambda value: value > 0,
            "staging.orders count must be greater than 0",
        ),
        (
            "no_duplicate_order_ids",
            "staging",
            fetch_one("""
                SELECT COUNT(*)
                FROM (
                    SELECT order_id
                    FROM staging.orders
                    GROUP BY order_id
                    HAVING COUNT(*) > 1
                ) duplicates;
            """),
            lambda value: value == 0,
            "order_id must be unique",
        ),
        (
            "no_invalid_order_status",
            "staging",
            fetch_one("""
                SELECT COUNT(*)
                FROM staging.orders
                WHERE status NOT IN ('completed', 'cancelled', 'returned');
            """),
            lambda value: value == 0,
            "status must be completed, cancelled, or returned",
        ),
        (
            "no_missing_customer_references",
            "staging",
            fetch_one("""
                SELECT COUNT(*)
                FROM staging.orders o
                LEFT JOIN staging.customers c
                    ON o.customer_id = c.customer_id
                WHERE c.customer_id IS NULL;
            """),
            lambda value: value == 0,
            "every order must match a customer",
        ),
        (
            "no_missing_product_references",
            "staging",
            fetch_one("""
                SELECT COUNT(*)
                FROM staging.orders o
                LEFT JOIN staging.products p
                    ON o.product_id = p.product_id
                WHERE p.product_id IS NULL;
            """),
            lambda value: value == 0,
            "every order must match a product",
        ),
    ]

    failed = []

    for check_name, layer_name, actual_value, rule, expected_rule in checks:
        passed = rule(actual_value)
        log_quality_check(run_id, check_name, layer_name, passed, actual_value, expected_rule)

        if not passed:
            failed.append(check_name)

    if failed:
        raise ValueError(f"Staging quality checks failed: {failed}")


def build_dimension_tables(**context):
    sql = """
    INSERT INTO warehouse.dim_customers (
        customer_id,
        customer_name,
        country,
        signup_date
    )
    SELECT
        customer_id,
        customer_name,
        country,
        signup_date
    FROM staging.customers;

    INSERT INTO warehouse.dim_products (
        product_id,
        product_name,
        category,
        price
    )
    SELECT
        product_id,
        product_name,
        category,
        price
    FROM staging.products;

    INSERT INTO warehouse.dim_date (
        date_key,
        full_date,
        year,
        month,
        month_name,
        quarter
    )
    SELECT
        TO_CHAR(d::DATE, 'YYYYMMDD')::INTEGER AS date_key,
        d::DATE AS full_date,
        EXTRACT(YEAR FROM d)::INTEGER AS year,
        EXTRACT(MONTH FROM d)::INTEGER AS month,
        TO_CHAR(d, 'Month') AS month_name,
        EXTRACT(QUARTER FROM d)::INTEGER AS quarter
    FROM GENERATE_SERIES(
        (SELECT MIN(order_date) FROM staging.orders),
        (SELECT MAX(order_date) FROM staging.orders),
        INTERVAL '1 day'
    ) AS d;
    """
    run_sql(sql)


def build_fact_sales(**context):
    sql = """
    INSERT INTO warehouse.fact_sales (
        order_id,
        customer_key,
        product_key,
        date_key,
        payment_method,
        quantity,
        unit_price,
        gross_amount,
        payment_amount,
        order_status,
        payment_status
    )
    SELECT
        o.order_id,
        c.customer_key,
        p.product_key,
        dd.date_key,
        pay.payment_method,
        o.quantity,
        p.price AS unit_price,
        o.quantity * p.price AS gross_amount,
        pay.amount AS payment_amount,
        o.status AS order_status,
        pay.payment_status
    FROM staging.orders o
    JOIN warehouse.dim_customers c
        ON o.customer_id = c.customer_id
    JOIN warehouse.dim_products p
        ON o.product_id = p.product_id
    JOIN warehouse.dim_date dd
        ON o.order_date = dd.full_date
    LEFT JOIN staging.payments pay
        ON o.order_id = pay.order_id
    WHERE o.status = 'completed'
      AND COALESCE(pay.payment_status, 'paid') = 'paid';
    """
    run_sql(sql)


def run_warehouse_quality_checks(**context):
    run_id = get_run_id(context)

    checks = [
        (
            "fact_sales_not_empty",
            "warehouse",
            fetch_one("SELECT COUNT(*) FROM warehouse.fact_sales;"),
            lambda value: value > 0,
            "warehouse.fact_sales count must be greater than 0",
        ),
        (
            "no_negative_revenue",
            "warehouse",
            fetch_one("SELECT COUNT(*) FROM warehouse.fact_sales WHERE gross_amount < 0;"),
            lambda value: value == 0,
            "gross_amount must not be negative",
        ),
        (
            "fact_sales_unique_orders",
            "warehouse",
            fetch_one("""
                SELECT COUNT(*)
                FROM (
                    SELECT order_id
                    FROM warehouse.fact_sales
                    GROUP BY order_id
                    HAVING COUNT(*) > 1
                ) duplicates;
            """),
            lambda value: value == 0,
            "each order_id must appear once in fact_sales",
        ),
    ]

    failed = []

    for check_name, layer_name, actual_value, rule, expected_rule in checks:
        passed = rule(actual_value)
        log_quality_check(run_id, check_name, layer_name, passed, actual_value, expected_rule)

        if not passed:
            failed.append(check_name)

    if failed:
        raise ValueError(f"Warehouse quality checks failed: {failed}")


def build_data_marts(**context):
    sql = """
    INSERT INTO mart.sales_by_country (
        country,
        total_orders,
        total_quantity,
        total_revenue
    )
    SELECT
        c.country,
        COUNT(f.order_id)::INTEGER AS total_orders,
        SUM(f.quantity)::INTEGER AS total_quantity,
        SUM(f.gross_amount)::NUMERIC(12, 2) AS total_revenue
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_customers c
        ON f.customer_key = c.customer_key
    GROUP BY c.country
    ORDER BY total_revenue DESC;

    INSERT INTO mart.sales_by_category (
        category,
        total_orders,
        total_quantity,
        total_revenue
    )
    SELECT
        p.category,
        COUNT(f.order_id)::INTEGER AS total_orders,
        SUM(f.quantity)::INTEGER AS total_quantity,
        SUM(f.gross_amount)::NUMERIC(12, 2) AS total_revenue
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_products p
        ON f.product_key = p.product_key
    GROUP BY p.category
    ORDER BY total_revenue DESC;

    INSERT INTO mart.monthly_revenue (
        year,
        month,
        total_orders,
        total_revenue
    )
    SELECT
        d.year,
        d.month,
        COUNT(f.order_id)::INTEGER AS total_orders,
        SUM(f.gross_amount)::NUMERIC(12, 2) AS total_revenue
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_date d
        ON f.date_key = d.date_key
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month;

    INSERT INTO mart.customer_revenue (
        customer_id,
        customer_name,
        country,
        total_orders,
        total_revenue
    )
    SELECT
        c.customer_id,
        c.customer_name,
        c.country,
        COUNT(f.order_id)::INTEGER AS total_orders,
        SUM(f.gross_amount)::NUMERIC(12, 2) AS total_revenue
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_customers c
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_id, c.customer_name, c.country
    ORDER BY total_revenue DESC;
    """
    run_sql(sql)


def export_table_to_csv(query, output_filename):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            headers = [desc[0] for desc in cur.description]

    with open(output_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"Exported {output_filename}")


def export_analytics_outputs(**context):
    export_table_to_csv(
        "SELECT * FROM mart.sales_by_country ORDER BY total_revenue DESC;",
        "sales_by_country.csv",
    )
    export_table_to_csv(
        "SELECT * FROM mart.sales_by_category ORDER BY total_revenue DESC;",
        "sales_by_category.csv",
    )
    export_table_to_csv(
        "SELECT * FROM mart.monthly_revenue ORDER BY year, month;",
        "monthly_revenue.csv",
    )
    export_table_to_csv(
        "SELECT * FROM mart.customer_revenue ORDER BY total_revenue DESC;",
        "customer_revenue.csv",
    )


def finish_pipeline(**context):
    run_id = get_run_id(context)

    sql = """
    UPDATE audit.pipeline_runs
    SET
        status = 'success',
        finished_at = NOW(),
        notes = 'Pipeline completed successfully.'
    WHERE pipeline_run_id = %s;
    """
    run_sql(sql, (run_id,))


default_args = {
    "owner": "abdullah",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="retail_data_warehouse_pipeline",
    description="Dockerized Airflow ETL and data warehouse pipeline with PostgreSQL",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["portfolio", "data-engineering", "etl", "data-warehouse"],
) as dag:

    create_objects = PythonOperator(
        task_id="create_database_objects",
        python_callable=create_database_objects,
    )

    start = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline,
    )

    extract = PythonOperator(
        task_id="extract_raw_csv_files",
        python_callable=extract_raw_csv_files,
    )

    staging = PythonOperator(
        task_id="clean_and_load_staging",
        python_callable=clean_and_load_staging,
    )

    staging_checks = PythonOperator(
        task_id="run_staging_quality_checks",
        python_callable=run_staging_quality_checks,
    )

    dimensions = PythonOperator(
        task_id="build_dimension_tables",
        python_callable=build_dimension_tables,
    )

    fact = PythonOperator(
        task_id="build_fact_sales",
        python_callable=build_fact_sales,
    )

    warehouse_checks = PythonOperator(
        task_id="run_warehouse_quality_checks",
        python_callable=run_warehouse_quality_checks,
    )

    marts = PythonOperator(
        task_id="build_data_marts",
        python_callable=build_data_marts,
    )

    export_outputs = PythonOperator(
        task_id="export_analytics_outputs",
        python_callable=export_analytics_outputs,
    )

    finish = PythonOperator(
        task_id="finish_pipeline",
        python_callable=finish_pipeline,
    )

    create_objects >> start >> extract >> staging >> staging_checks >> dimensions >> fact >> warehouse_checks >> marts >> export_outputs >> finish
