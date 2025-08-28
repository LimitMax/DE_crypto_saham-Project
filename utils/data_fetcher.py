import yfinance as yf
import pandas as pd
import logging

def fetch_data(symbol, start, end, interval="1h"):
    if start >= end:
        logging.warning(f"⚠️ Invalid range for {symbol}: start {start} >= end {end}")
        return pd.DataFrame()

    df = yf.download(symbol, start=start, end=end, interval=interval)
    if df.empty:
        logging.warning(f"⚠️ No data returned for {symbol} {start} - {end}")
        return pd.DataFrame()

    df.reset_index(inplace=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if c[0] else c[1] for c in df.columns]

    # fleksibel ambil kolom waktu
    ts_col = None
    if "Datetime" in df.columns:
        ts_col = "Datetime"
    elif "Date" in df.columns:
        ts_col = "Date"

    if ts_col is None:
        logging.error(f"❌ No time column found for {symbol}")
        return pd.DataFrame()

    df['date'] = df[ts_col].dt.date
    df['hourx'] = df[ts_col].dt.hour
    df['crypto'] = symbol

    curated = df[['date','hourx','crypto','Open','High','Low','Close','Volume']]
    logging.info(f"✅ {symbol}: fetched {len(curated)} rows ({start} → {end})")
    return curated
