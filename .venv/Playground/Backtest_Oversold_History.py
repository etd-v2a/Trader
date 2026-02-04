import pandas as pd
import pandas_ta_classic as ta
import yfinance as yf
from backtesting import Backtest, Strategy
import os


# --- 1. The Strategy Logic ---
class OversoldBounce(Strategy):
    rsi_period = 14
    rsi_limit = 30
    exit_rsi = 50
    sma_filter = 200

    def init(self):
        self.rsi = self.I(ta.rsi, self.data.Close.s, length=self.rsi_period)
        self.sma = self.I(ta.sma, self.data.Close.s, length=self.sma_filter)

    def next(self):
        price = self.data.Close[-1]

        if not self.position:
            # Buy if Oversold AND in Long Term Uptrend
            if self.rsi[-1] < self.rsi_limit:
                if len(self.sma) > 0 and price > self.sma[-1]:
                    self.buy()

        elif self.position:
            # Sell if RSI recovers
            if self.rsi[-1] > self.exit_rsi:
                self.position.close()


# --- 2. The Validation Engine ---
def validate_csv_list():
    # A. Setup File System
    filename = 'oversold_history.csv'
    plot_folder = 'plots'

    # Create the 'plots' folder if it doesn't exist
    if not os.path.exists(plot_folder):
        os.makedirs(plot_folder)
        print(f"Created new folder: {plot_folder}/")

    if not os.path.exists(filename):
        print(f"Error: '{filename}' not found. Run the daily scanner first.")
        return

    # B. Load and Prepare Data
    df_candidates = pd.read_csv(filename)

    # Get unique tickers to avoid re-testing the same stock twice
    unique_tickers = df_candidates['Ticker'].unique()

    print(f"--- Analyzing {len(unique_tickers)} Candidates ---")
    print(f"Plots will be saved to: {plot_folder}/")

    report_card = []

    for ticker in unique_tickers:
        try:
            # Get the scan date (use the most recent one if duplicates exist)
            # We filter the dataframe for this ticker, pick the last row, get 'Scan_Date'
            scan_date = df_candidates[df_candidates['Ticker'] == ticker].iloc[-1]['Scan_Date']

            # 1. Download Data (3 Years)
            data = yf.download(ticker, period="3y", progress=False)

            # Fix multi-index columns if they exist
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = [col[0] for col in data.columns]

            if len(data) < 250:
                print(f"Skipping {ticker}: Not enough data.")
                continue

            # 2. Run Backtest
            bt = Backtest(data, OversoldBounce, cash=10000, commission=.002)
            stats = bt.run()

            # 3. Collect Metrics
            avg_duration = stats['Avg. Trade Duration']
            avg_days = avg_duration.days if hasattr(avg_duration, 'days') else 0
            pf = stats['Profit Factor']

            report_card.append({
                'Ticker': ticker,
                'Found On': scan_date,
                'Profit Factor': round(pf, 2),
                'Win Rate (%)': round(stats['Win Rate [%]'], 1),
                'Return (%)': round(stats['Return [%]'], 1),
                'Avg Days Held': avg_days,
                '# Trades': stats['# Trades']
            })

            print(f"Processed {ticker}: PF = {round(pf, 2)}")

            # 4. PLOT AND SAVE (Only if Profitable)
            if pf > 1.0:
                # Construct filename: plots/NVDA_2024-02-04_OversoldBounce
                strategy_name = "OversoldBounce"
                safe_filename = f"{plot_folder}/{ticker}_{scan_date}_{strategy_name}"

                # Backtesting.py appends .html automatically usually, but we ensure path is clean
                bt.plot(filename=safe_filename, open_browser=False)

        except Exception as e:
            print(f"Failed to analyze {ticker}: {e}")

    # --- 3. Final Report ---
    if report_card:
        results_df = pd.DataFrame(report_card)
        results_df = results_df.sort_values(by='Profit Factor', ascending=False)

        print("\n" + "=" * 60)
        print("CANDIDATE LEADERBOARD (Sorted by Profit Factor)")
        print("=" * 60)
        print(results_df.to_string(index=False))

        results_df.to_csv('candidate_validation_v3.csv', index=False)
        print(f"\nAnalysis complete. Check the '{plot_folder}' folder for charts.")
    else:
        print("No valid results generated.")


if __name__ == "__main__":
    validate_csv_list()