import pandas as pd
import pandas_ta_classic as ta
import yfinance as yf
from backtesting import Backtest, Strategy
from datetime import datetime, timedelta


# --- 1. The Strategy Logic (Same as before) ---
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

        # Buy Logic
        if not self.position:
            if self.rsi[-1] < self.rsi_limit:
                # Only buy if price is > 200 SMA (Long Term Uptrend)
                if len(self.sma) > 0 and price > self.sma[-1]:
                    self.buy()

        # Sell Logic
        elif self.position:
            if self.rsi[-1] > self.exit_rsi:
                self.position.close()


# --- 2. The Loop (Browsing your CSV) ---
def validate_csv_list():
    # Load your Finviz list
    try:
        df_candidates = pd.read_csv('oversold_history.csv')
    except FileNotFoundError:
        print("Error: 'oversold_history.csv' not found. Run the scanner first.")
        return

    # Get unique tickers (in case you ran the scan multiple times)
    tickers = df_candidates['Ticker'].unique()
    print(f"Analyzing {len(tickers)} candidates from your list...\n")

    report_card = []

    for ticker in tickers:
        try:
            # A. Download 3 years of history for this candidate
            # We need history to see how it BEHAVED in the past
            data = yf.download(ticker, period="3y", progress=False)

            # Clean multi-index headers if present (yfinance quirk)
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = [col[0] for col in data.columns]

            # Check if we have enough data
            if len(data) < 250:
                print(f"Skipping {ticker}: Not enough data.")
                continue

            # B. Run the Backtest
            bt = Backtest(data, OversoldBounce, cash=10000, commission=.002)
            stats = bt.run()

            # C. Log the Results
            # We care about: Win Rate, Profit Factor, and Return
            report_card.append({
                'Ticker': ticker,
                'Win Rate (%)': round(stats['Win Rate [%]'], 1),
                '# Trades': stats['# Trades'],
                'Return (%)': round(stats['Return [%]'], 1),
                'Profit Factor': round(stats['Profit Factor'], 2)
            })
            print(f"Processed {ticker}...")

        except Exception as e:
            print(f"Failed to analyze {ticker}: {e}")

    # --- 3. Display the "Leaderboard" ---
    if report_card:
        results_df = pd.DataFrame(report_card)

        # Sort by Win Rate (Highest first)
        results_df = results_df.sort_values(by='Win Rate (%)', ascending=False)

        print("\n--- CANDIDATE VALIDATION REPORT ---")
        print("Which of your oversold stocks are actually good traders?")
        print(results_df.to_string(index=False))

        # Optional: Save validation to file
        results_df.to_csv('candidate_validation.csv', index=False)
    else:
        print("No valid results generated.")


if __name__ == "__main__":
    validate_csv_list()