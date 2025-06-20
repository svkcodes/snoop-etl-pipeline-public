# Data Transformation

# etl/tranform.py

import pandas as pd
import logging

def clean_and_deduplicate(df):
    """
    - Removes PII fields
    - Deduplicates based on customerId + transactionId, keeping latest transactionDate

    Returns:
        cleaned_df (pd.DataFrame)
    """
    logging.info("Cleaning and deduplicating transactions...")

    # Drop PII
    if "customerName" in df.columns:
        df = df.drop(columns=["customerName"])
        logging.info("Removed PII field: customerName")

    # Parse transactionDate
    df["transactionDate"] = pd.to_datetime(df["transactionDate"], errors="coerce")

    # Remove rows with invalid dates
    before = len(df)
    df = df.dropna(subset=["transactionDate"])
    after = len(df)
    logging.info(f"Removed {before - after} rows with invalid transactionDate")

    # Deduplicate records
    df = df.sort_values("transactionDate").drop_duplicates(subset=["customerId", "transactionId"], keep="last")
    logging.info(f"DataFrame cleaned and deduplicated. Remaining records: {len(df)}")

    return df


def get_customers_table(df):
    """
    Prepares the customers table: one row per customer with latest transactionDate

    Returns:
        customers_df (pd.DataFrame)
    """
    logging.info("Generating customers summary table...")

    df["transactionDate"] = pd.to_datetime(df["transactionDate"], errors="coerce")
    df = df.dropna(subset=["transactionDate"])

    customers_df = df.groupby("customerId").agg(
        latestTransactionDate=("transactionDate", "max")
    ).reset_index()

    logging.info(f"Generated customers table with {len(customers_df)} unique customers.")
    return customers_df
