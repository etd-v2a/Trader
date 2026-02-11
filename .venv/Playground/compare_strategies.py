import pandas as pd
import yfinance as yf
import pandas_ta_classic as ta
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import numpy as np

# --- CONFIGURATION ---
PROJECT_ID = 'invest-app-479915'
HISTORY_DAYS = 365
STOP_LOSS_PCT = 0.10
PATTERN_LOOKAHEAD = 5

if not firebase_admin._apps:
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred, {'projectId': PROJECT_ID})
db = firestore.client()


def calculate_metrics(trades_list, strategy_name):
    if not trades_list:
        return {'Strategy': strategy_name, 'Trades': 0, 'Win Rate': '0.0%',
                'Avg Return': '0.00%', 'Profit Factor': 0.00}

    df = pd.DataFrame(trades_list)
    winners = df[df['Return'] > 0]
    gross_profit = df[df['Return'] > 0]['Return'].sum()
    gross_loss = abs(df[df['Return'] < 0]['Return'].sum())
    pf = round(gross_profit / gross_loss, 2) if gross_loss > 0 else 99.99

    return {
        'Strategy': strategy_name,
        'Trades': len(df),
        'Win Rate': f"{round((len(winners) / len(df)) * 100, 1)}%",
        'Avg Return': f"{round(df['Return'].mean(), 2)}%",
        'Profit Factor': pf
    }


def detect_pattern(curr, prev):
    if pd.isna(curr['Open']) or pd.isna(prev['Open']): return None

    open_p, close_p = curr['Open'], curr['Close']
    high_p, low_p = curr['High'], curr['Low']
    body = abs(close_p - open_p)
    rng = high_p - low_p

    if rng == 0: return None

    upper_shadow = high_p - max(open_p, close_p)
    lower_shadow = min(open_p, close_p) - low_p

    # --- DOJI FAMILY ---
    if body <= (0.1 * rng):
        if upper_shadow <= (0.3 * rng):
            return 'Dragonfly Doji'
        elif lower_shadow <= (0.3 * rng):
            return 'Gravestone Doji'
        else:
            return 'Standard Doji'

    # --- HAMMER ---
    if (body <= 0.3 * rng) and (lower_shadow >= 2 * body) and (upper_shadow <= body):
        return 'Hammer'

    # --- ENGULFING ---
    prev_open, prev_close = prev['Open'], prev['Close']
    if (prev_close < prev_open) and (close_p > open_p):
        if (open_p <= prev_close) and (close_p >= prev_open):
            return 'Engulfing'

    return None


def apply_stop_loss(entry_price, future_df, target_exit_price, target_ret):
    sl_price = entry_price * (1 - STOP_LOSS_PCT)
    sl_hit = future_df[future_df['Low'] < sl_price]
    if not sl_hit.empty: return -100 * STOP_LOSS_PCT
    return target_ret


def run_full_deduped_fixed():
    print(f"--- 🧬 FULL DEDUPLICATED INSPECTION (FIXED) 🧬 ---")

    docs = db.collection('oversold_events').stream()
    df_signals = pd.DataFrame([d.to_dict() for d in docs])
    df_signals['Scan_Date'] = pd.to_datetime(df_signals['Scan_Date'])
    df_signals = df_signals.sort_values(by='Scan_Date')

    start_date = datetime.now() - timedelta(days=HISTORY_DAYS)
    df_signals = df_signals[df_signals['Scan_Date'] > start_date]

    unique_tickers = df_signals['Ticker'].unique()
    print(f"Scanning {len(unique_tickers)} stocks...")

    market_data = {}
    for ticker in unique_tickers:
        try:
            df = yf.download(ticker, start=start_date - timedelta(days=300), progress=False)
            if len(df) > 200:
                if isinstance(df.columns, pd.MultiIndex): df.columns = [c[0] for c in df.columns]
                df['RSI'] = ta.rsi(df['Close'], length=14)
                df['SMA_200'] = ta.sma(df['Close'], length=200)
                market_data[ticker] = df
        except:
            pass

    strategies = {
        'Runner (200 SMA Filter)': [],
        'Engulfing': [],
        'Raw Doji': [],
        'Confirmed Doji': [],
        'Raw Hammer': [],
        'Confirmed Hammer': []
    }

    # Deduplication Trackers
    last_exits = {k: {} for k in strategies.keys()}
    inspection_list = []

    print("Analyzing charts...")

    for idx, row in df_signals.iterrows():
        ticker = row['Ticker']
        scan_date = row['Scan_Date']

        if ticker not in market_data: continue
        df = market_data[ticker]
        if scan_date not in df.index: continue

        scan_idx = df.index.get_loc(scan_date)
        if scan_idx >= len(df) - 1: continue

        # --- 1. RUNNER STRATEGY ---
        curr_close = df.iloc[scan_idx]['Close']
        curr_sma = df.iloc[scan_idx]['SMA_200']
        entry_date = df.index[scan_idx + 1]
        s_name = 'Runner (200 SMA Filter)'

        if (last_exits[s_name].get(ticker) is None or entry_date > last_exits[s_name].get(ticker)):
            if not pd.isna(curr_sma) and curr_close > curr_sma:
                entry_price = df.iloc[scan_idx + 1]['Open']
                future = df.iloc[scan_idx + 1:]

                mask_50 = future['RSI'] > 50
                if mask_50.any():
                    exit_date = mask_50.idxmax()
                    price_50 = df.loc[exit_date, 'Close']
                else:
                    exit_date = future.index[-1]
                    price_50 = future.iloc[-1]['Close']

                mask_70 = future['RSI'] > 70
                if mask_70.any():
                    exit_date_70 = mask_70.idxmax()
                    price_70 = df.loc[exit_date_70, 'Close']
                    if exit_date_70 > exit_date: exit_date = exit_date_70
                else:
                    price_70 = future.iloc[-1]['Close']

                avg_exit = (0.75 * price_50) + (0.25 * price_70)
                ret_runner = ((avg_exit - entry_price) / entry_price) * 100
                final_runner = apply_stop_loss(entry_price, future, avg_exit, ret_runner)

                strategies[s_name].append({'Return': final_runner})
                last_exits[s_name][ticker] = exit_date

                inspection_list.append({'Ticker': ticker, 'Date': scan_date.strftime('%Y-%m-%d'), 'Pattern': 'RUNNER',
                                        'Result': final_runner})

        # --- 2. PATTERNS ---
        for i in range(scan_idx, min(scan_idx + PATTERN_LOOKAHEAD + 1, len(df) - 2)):
            if i == 0: continue

            curr = df.iloc[i]
            prev = df.iloc[i - 1]
            pat = detect_pattern(curr, prev)  # Returns 'Dragonfly Doji', 'Standard Doji', etc.

            if not pat: continue

            pat_date_str = df.index[i].strftime('%Y-%m-%d')
            entry_date = df.index[i + 1]  # Entry is Next Open

            # Common Exit Logic
            raw_entry = df.iloc[i + 1]['Open']
            raw_future = df.iloc[i + 1:]
            r_mask = raw_future['RSI'] > 50
            if r_mask.any():
                raw_exit_date = r_mask.idxmax(); raw_exit = df.loc[raw_exit_date, 'Close']
            else:
                raw_exit_date = raw_future.index[-1]; raw_exit = raw_future.iloc[-1]['Close']
            raw_ret = ((raw_exit - raw_entry) / raw_entry) * 100
            raw_final = apply_stop_loss(raw_entry, raw_future, raw_exit, raw_ret)

            # --- ENGULFING ---
            if pat == 'Engulfing':
                s_name = 'Engulfing'
                if (last_exits[s_name].get(ticker) is None or entry_date > last_exits[s_name].get(ticker)):
                    strategies[s_name].append({'Return': raw_final})
                    last_exits[s_name][ticker] = raw_exit_date
                    inspection_list.append(
                        {'Ticker': ticker, 'Date': pat_date_str, 'Pattern': 'Engulfing', 'Result': raw_final})

            # --- DOJI (ANY TYPE) ---
            if 'Doji' in pat:  # <--- FIXED: Matches Dragonfly, Gravestone, Standard

                # Raw Doji
                s_name = 'Raw Doji'
                if (last_exits[s_name].get(ticker) is None or entry_date > last_exits[s_name].get(ticker)):
                    strategies[s_name].append({'Return': raw_final})
                    last_exits[s_name][ticker] = raw_exit_date
                    # We save the SPECIFIC type (e.g. Dragonfly) for your inspection
                    inspection_list.append(
                        {'Ticker': ticker, 'Date': pat_date_str, 'Pattern': f"Raw {pat}", 'Result': raw_final})

                # Confirmed Doji
                conf_candle = df.iloc[i + 1]
                if conf_candle['High'] > df.iloc[i]['High']:
                    c_entry = conf_candle['Close']
                    c_future = df.iloc[i + 2:]
                    if not c_future.empty:
                        c_mask = c_future['RSI'] > 50
                        if c_mask.any():
                            c_exit_date = c_mask.idxmax(); c_exit = df.loc[c_exit_date, 'Close']
                        else:
                            c_exit_date = c_future.index[-1]; c_exit = c_future.iloc[-1]['Close']
                        c_ret = ((c_exit - c_entry) / c_entry) * 100
                        c_final = apply_stop_loss(c_entry, c_future, c_exit, c_ret)

                        s_name = 'Confirmed Doji'
                        conf_date = df.index[i + 1]
                        if (last_exits[s_name].get(ticker) is None or conf_date > last_exits[s_name].get(ticker)):
                            strategies[s_name].append({'Return': c_final})
                            last_exits[s_name][ticker] = c_exit_date
                            inspection_list.append(
                                {'Ticker': ticker, 'Date': pat_date_str, 'Pattern': f"Conf {pat}", 'Result': c_final})

            # --- HAMMER ---
            if pat == 'Hammer':
                # Raw Hammer
                s_name = 'Raw Hammer'
                if (last_exits[s_name].get(ticker) is None or entry_date > last_exits[s_name].get(ticker)):
                    strategies[s_name].append({'Return': raw_final})
                    last_exits[s_name][ticker] = raw_exit_date
                    inspection_list.append(
                        {'Ticker': ticker, 'Date': pat_date_str, 'Pattern': 'Raw Hammer', 'Result': raw_final})

                # Confirmed Hammer
                conf_candle = df.iloc[i + 1]
                if conf_candle['High'] > df.iloc[i]['High']:
                    c_entry = conf_candle['Close']
                    c_future = df.iloc[i + 2:]
                    if not c_future.empty:
                        c_mask = c_future['RSI'] > 50
                        if c_mask.any():
                            c_exit_date = c_mask.idxmax(); c_exit = df.loc[c_exit_date, 'Close']
                        else:
                            c_exit_date = c_future.index[-1]; c_exit = c_future.iloc[-1]['Close']
                        c_ret = ((c_exit - c_entry) / c_entry) * 100
                        c_final = apply_stop_loss(c_entry, c_future, c_exit, c_ret)

                        s_name = 'Confirmed Hammer'
                        conf_date = df.index[i + 1]
                        if (last_exits[s_name].get(ticker) is None or conf_date > last_exits[s_name].get(ticker)):
                            strategies[s_name].append({'Return': c_final})
                            last_exits[s_name][ticker] = c_exit_date
                            inspection_list.append(
                                {'Ticker': ticker, 'Date': pat_date_str, 'Pattern': 'Conf Hammer', 'Result': c_final})

    # --- REPORTING ---
    summary = [calculate_metrics(v, k) for k, v in strategies.items()]
    df_res = pd.DataFrame(summary).sort_values(by='Profit Factor', ascending=False)

    print("\n" + "=" * 80)
    print("📊 FULL DEDUPLICATED RESULTS 📊")
    print("=" * 80)
    print(df_res.to_string(index=False))

    df_inspect = pd.DataFrame(inspection_list)
    if not df_inspect.empty:
        df_inspect['Result'] = df_inspect['Result'].round(2)
        print("\n--- 🟢 TOP 15 WINNERS (Green Flags) ---")
        print(df_inspect.sort_values(by='Result', ascending=False).head(15).to_string(index=False))

        print("\n--- 🔴 TOP 15 LOSERS (Red Flags) ---")
        print(df_inspect.sort_values(by='Result', ascending=True).head(15).to_string(index=False))

        df_inspect.to_csv('full_inspection_list.csv', index=False)
    else:
        print("No trades found.")


if __name__ == "__main__":
    run_full_deduped_fixed()