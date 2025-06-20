#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data validation logic for transactions
"""

import pandas as pd
import logging

# Configure logging if not set globally
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Allowed currency codes (based on the Data Dictionary)
ALLOWED_CURRENCIES = {"USD", "EUR", "GBP"}

def validate_dataframe(df):
    """
    Perform data quality checks:
    - Missing values in any column
    - Valid transactionDate
    - Allowed currencies
    - Duplicate transactionId detection

    Returns:
        valid_df, invalid_df (with 'error_reason')
    """
    logging.info(" Starting data validation")
    
    # Flatten nested structure if necessary
    df = pd.json_normalize(df["transactions"])
    df["error_reason"] = ""

    # Missing values
    missing_mask = df.isnull().any(axis=1)
    df.loc[missing_mask, "error_reason"] += "Missing values; "
    logging.info(f" Found {missing_mask.sum()} rows with missing values")

    # Date validation
    df["transactionDate_parsed"] = pd.to_datetime(df["transactionDate"], errors="coerce")
    invalid_dates = df["transactionDate_parsed"].isnull()
    df.loc[invalid_dates, "error_reason"] += "Invalid transactionDate; "
    logging.info(f" Found {invalid_dates.sum()} rows with invalid transactionDate")

    # Currency validation
    invalid_currency = ~df["currency"].isin(ALLOWED_CURRENCIES)
    df.loc[invalid_currency, "error_reason"] += "Invalid currency; "
    logging.info(f" Found {invalid_currency.sum()} rows with invalid currency")

    # Duplicate detection
    df["is_duplicate"] = df.duplicated(subset=["transactionId"], keep=False)
    df.loc[df["is_duplicate"], "error_reason"] += "Duplicate transactionId; "
    logging.info(f" Found {df['is_duplicate'].sum()} duplicate transactionId entries")

    # Separate valid and invalid
    invalid_df = df[df["error_reason"] != ""].drop_duplicates(
        subset=["customerId", "transactionId", "error_reason"]
    ).copy()
    valid_df = df[df["error_reason"] == ""].copy()

    logging.info(f" Valid records: {len(valid_df)}")
    logging.info(f" Invalid records: {len(invalid_df)}")

    return (
        valid_df.drop(columns=["transactionDate_parsed", "is_duplicate", "error_reason"]),
        invalid_df.drop(columns=["transactionDate_parsed", "is_duplicate"]).reset_index(drop=True)
    )
