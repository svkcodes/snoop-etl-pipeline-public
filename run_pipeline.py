#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 17 08:23:28 2025

@author: swara
"""

import logging
from etl.ingest import read_json_to_dataframe
from etl.validate import validate_dataframe
from etl.transform import clean_and_deduplicate, get_customers_table
from etl.load import upsert_transactions, upsert_customers, insert_errors, log_table_counts

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def main():
    logging.info("🚀 Starting ETL pipeline...")

    # Step 1: Ingest
    logging.info("📥 Step 1: Ingesting data")
    df = read_json_to_dataframe("data")

    # Step 2: Validate
    logging.info("🧪 Step 2: Validating data")
    valid_df, invalid_df = validate_dataframe(df)
    logging.info(f" Valid records: {len(valid_df)}")
    logging.info(f" Invalid records: {len(invalid_df)}")

    if not invalid_df.empty:
        invalid_df.to_csv("logs/invalid_records.csv", index=False)
        logging.info(" Invalid records saved to logs/invalid_records.csv")

    # Step 3: Transform
    logging.info("🔧 Step 3: Transforming data")
    cleaned_df = clean_and_deduplicate(valid_df)
    customers_df = get_customers_table(cleaned_df)

    cleaned_df.to_csv("logs/cleaned_transactions.csv", index=False)
    customers_df.to_csv("logs/customers_summary.csv", index=False)
    logging.info(" Cleaned and customer data saved to logs/")

    # Step 4: Load
    logging.info(" Step 4: Loading into PostgreSQL")
    upsert_transactions(cleaned_df)
    upsert_customers(customers_df)
    insert_errors(invalid_df)

    logging.info(" ETL pipeline completed successfully")

    # Summary
    log_table_counts()

if __name__ == "__main__":
    main()
