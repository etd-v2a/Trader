import pandas as pd
from finvizfinance.screener.technical import Technical
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore
import time

# --- CONFIGURATION ---
PROJECT_ID = 'invest-app-479915'
COLLECTION_NAME = 'pattern_signals'

if not firebase_admin._apps:
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred, {'projectId': PROJECT_ID})

db = firestore.client()


def run_reliable_scan():
    print(f"--- 🕵️ RELIABLE PATTERN SCANNER (Library Method) 🕵️ ---")

    # We only use patterns supported by the 'Candlestick' filter in the library
    # to avoid the "Invalid Filter" crashes.
    patterns_to_scan = [
        'Hammer',
        'Dragonfly Doji',
        'Marubozu White',  # Strong buying pressure (Momentum)
        'Long Lower Shadow'  # Rejection of lows (Dip Buy)
    ]

    batch = db.batch()
    total_count = 0
    scan_date = datetime.today().strftime('%Y-%m-%d')
    scan_timestamp = datetime.now()

    for pattern_name in patterns_to_scan:
        print(f"\nSearching for: {pattern_name}...")

        # Define Filters
        # 'Market Cap.': '+Mid (over $2bln)' includes Mid, Large, and Mega caps.
        filters_dict = {
            'Market Cap.': '+Mid (over $2bln)',
            '200-Day Simple Moving Average': 'Price above SMA200',
            'Candlestick': pattern_name
        }

        try:
            ftech = Technical()
            ftech.set_filter(filters_dict=filters_dict)
            df_results = ftech.screener_view()

            # CHECK: Did we find anything?
            if df_results is None or df_results.empty:
                print(f" -> No stocks found.")
                continue

            print(f" -> Found {len(df_results)} stocks.")

            for index, row in df_results.iterrows():
                ticker = row['Ticker']

                # Deduplication ID
                doc_id = f"{ticker}_{scan_date}_{pattern_name.replace(' ', '')}"
                doc_ref = db.collection(COLLECTION_NAME).document(doc_id)

                data = row.to_dict()

                # Clean Data
                if 'RSI' in data and data['RSI'] != '-':
                    try:
                        data['RSI'] = float(data['RSI'])
                    except:
                        data['RSI'] = None
                else:
                    data['RSI'] = None

                data['Scan_Date'] = scan_date
                data['Created_At'] = scan_timestamp
                data['Pattern_Type'] = pattern_name
                data['Trend_Status'] = 'Above SMA200'

                batch.set(doc_ref, data)
                total_count += 1

                if total_count % 400 == 0:
                    batch.commit()
                    batch = db.batch()

            # Sleep to prevent rate limiting
            time.sleep(1)

        except Exception as e:
            print(f"Error scanning for {pattern_name}: {e}")

    # Final Commit
    if total_count > 0:
        batch.commit()

    print(f"\nSUCCESS! Processed {total_count} alerts.")
    print("Check your Firestore database.")


if __name__ == "__main__":
    run_reliable_scan()