#Database configuration and loading data into the tables

# etl/load.py

import json
import time
import psycopg2
import os
import logging

def get_connection(retries=5, delay=5):
    db_host = os.environ["DB_HOST"]
    db_port = int(os.environ["DB_PORT"])
    db_name = os.environ["DB_NAME"]
    db_user = os.environ["DB_USER"]
    db_password = os.environ["DB_PASSWORD"]

    for attempt in range(retries):
        try:
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password
            )
            if attempt > 0:
                logging.info(f"Connected to PostgreSQL after retry {attempt + 1}")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < retries - 1:
                logging.warning(f"Connection attempt {attempt + 1} failed. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error("All retry attempts failed. Could not connect to database.")
                raise RuntimeError("Failed to connect to the database after multiple retries") from e


def upsert_transactions(df):
    logging.info("Upserting transactions...")
    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO transactions (
                customerId, transactionId, transactionDate, sourceDate, 
                merchantId, categoryId, amount, description, currency
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customerId, transactionId)
            DO UPDATE SET
                transactionDate = EXCLUDED.transactionDate,
                sourceDate = EXCLUDED.sourceDate,
                merchantId = EXCLUDED.merchantId,
                categoryId = EXCLUDED.categoryId,
                amount = EXCLUDED.amount,
                description = EXCLUDED.description,
                currency = EXCLUDED.currency,
                updated_at = CURRENT_TIMESTAMP
            WHERE transactions.sourceDate < EXCLUDED.sourceDate;
        """, (
            row["customerId"], row["transactionId"], row["transactionDate"],
            row["sourceDate"], row["merchantId"], row["categoryId"],
            float(row["amount"]), row["description"], row["currency"]
        ))

    conn.commit()
    logging.info("Transactions table updated.")
    cur.close()
    conn.close()


def upsert_customers(df):
    logging.info("Upserting customers...")
    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO customers (customerId, latestTransactionDate)
            VALUES (%s, %s)
            ON CONFLICT (customerId)
            DO UPDATE SET
                latestTransactionDate = EXCLUDED.latestTransactionDate,
                updated_at = CURRENT_TIMESTAMP
            WHERE customers.latestTransactionDate < EXCLUDED.latestTransactionDate;
        """, tuple(row))

    conn.commit()
    logging.info("Customers table updated.")
    cur.close()
    conn.close()


def insert_errors(df):
    if df.empty:
        logging.info("No errors to log.")
        return

    logging.info("Logging error records to error_log table...")
    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        raw_json = row.drop("error_reason").to_json()
        cur.execute("""
            INSERT INTO error_log (customerId, transactionId, error_reason, raw_data)
            VALUES (%s, %s, %s, %s)
        """, (
            row.get("customerId"), row.get("transactionId"),
            row.get("error_reason"), json.dumps(json.loads(raw_json))
        ))

    conn.commit()
    logging.info("Errors inserted into error_log table.")
    cur.close()
    conn.close()


def log_table_counts():
    logging.info("Fetching record counts from all tables...")
    conn = get_connection()
    cur = conn.cursor()

    for table in ["transactions", "customers", "error_log"]:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        count = cur.fetchone()[0]
        logging.info(f"Table '{table}': {count} records")

    cur.close()
    conn.close()

