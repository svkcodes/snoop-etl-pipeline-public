#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 17 07:30:15 2025

@author: swara
"""

# etl/ingest.py
import os
import pandas as pd
import logging

# Configure logging (if not already configured in main script)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def read_json_to_dataframe(data_dir="data"):
    """
    Reads all JSON files from a directory into a single DataFrame.

    Args:
        data_dir (str): Relative path to data folder.

    Returns:
        pd.DataFrame: Concatenated data.
    """
    all_data = []
    logging.info(f"Scanning directory: {data_dir} for JSON files...")

    for file in os.listdir(data_dir):
        if file.endswith(".json"):
            file_path = os.path.join(data_dir, file)
            try:
                df = pd.read_json(file_path)
                all_data.append(df)
                logging.info(f"Loaded {file} with {len(df)} records")
            except ValueError as e:
                logging.warning(f"Could not load {file}: {e}")

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        logging.info(f"Total records read: {len(combined_df)}")
        return combined_df
    else:
        logging.warning("No JSON files found or all failed to load.")
        return pd.DataFrame()

