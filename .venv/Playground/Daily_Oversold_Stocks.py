import pandas as pd
from finvizfinance.screener.overview import Overview
from datetime import datetime
import os


def run_daily_scan():
    print("--- Starting Daily Oversold Scan ---")

    # 1. Define your Finviz Filters (The "Requirements")
    # specific filters: Mid Cap+, Oversold (RSI < 30), Earnings not imminent
    filters_dict = {
        'Market Cap.': 'Mid ($2bln to $10bln)',
        'RSI (14)': 'Oversold (30)',
        'Average Volume': 'Over 500K',
        'Relative Volume': 'Over 1'  # Panic selling check
    }

    # 2. Initialize the Screener
    foverview = Overview()
    foverview.set_filter(filters_dict=filters_dict)

    # 3. Get the Data
    try:
        df_results = foverview.screener_view()
        print(f"Found {len(df_results)} candidates today.")
    except Exception as e:
        print(f"Error fetching data: {e}")
        return

    # 4. Add "Context" (Date and Analysis columns)
    # We add a 'Date' column so you know WHEN it was found
    df_results['Scan_Date'] = datetime.today().strftime('%Y-%m-%d')

    # 5. Save to CSV (Append mode)
    filename = 'oversold_history.csv'

    if os.path.exists(filename):
        # If file exists, load it, append new data, and save back
        # This prevents overwriting your history
        df_results.to_csv(filename, mode='a', header=False, index=False)
        print(f"Appended to {filename}")
    else:
        # If first run, create the file with headers
        df_results.to_csv(filename, mode='w', header=True, index=False)
        print(f"Created new database: {filename}")


if __name__ == "__main__":
    run_daily_scan()