import yfinance as yf
import pandas as pd
import datetime, json, os, pyodbc
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# ===============================
# CONFIG
# ===============================
CRYPTOS = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD"]
START = "2024-01-01"
END   = "2025-08-23"
INTERVAL = "1h"   # data per jam

# Load .env
load_dotenv()

# Env vars
BLOB_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
SQL_SERVER    = os.getenv("SQL_SERVER")
SQL_DATABASE  = os.getenv("SQL_DATABASE")
SQL_USERNAME  = os.getenv("SQL_USERNAME")
SQL_PASSWORD  = os.getenv("SQL_PASSWORD")
DRIVER        = '{ODBC Driver 18 for SQL Server}'

# ===============================
# UTILS
# ===============================

def to_scalar(val, cast_type=float):
    """Convert Pandas/numpy object ke scalar Python (float/int/None)."""
    if hasattr(val, "item"):
        val = val.item()
    if pd.isna(val):
        return None
    return cast_type(val) if cast_type is not None else val

# ===============================
# FUNGSI DB & BLOB
# ===============================

def connect_blob():
    return BlobServiceClient.from_connection_string(BLOB_CONN_STR)

def connect_sql():
    return pyodbc.connect(
        f"DRIVER={DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )

def create_tables(cursor):
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='CryptoPrice' AND xtype='U')
    CREATE TABLE CryptoPrice (
        id INT IDENTITY(1,1) PRIMARY KEY,
        date DATE NOT NULL,
        hourx INT NOT NULL,
        crypto NVARCHAR(20) NOT NULL,
        [Open] FLOAT, [High] FLOAT, [Low] FLOAT, [Close] FLOAT, [Volume] BIGINT,
        inserted_at DATETIME2 DEFAULT GETDATE(),
        CONSTRAINT uq_crypto UNIQUE (date,hourx,crypto)
    );
    """)
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DataQualityIssues' AND xtype='U')
    CREATE TABLE DataQualityIssues (
        id INT IDENTITY(1,1) PRIMARY KEY,
        ticker NVARCHAR(20),
        datetime DATETIME2,
        issue_type NVARCHAR(255),
        raw_data NVARCHAR(MAX),
        logged_at DATETIME2 DEFAULT GETDATE()
    );
    """)

# ===============================
# FETCH & SAVE
# ===============================

def fetch_data(symbol, start, end, interval):
    print(f"🔄 Mengambil data {symbol} dari {start} sampai {end} interval {interval}...")
    df = yf.download(symbol, start=start, end=end, interval=interval)
    df.reset_index(inplace=True)
    print(f"✅ {symbol}: {len(df)} baris diambil")
    return df

def save_raw_to_blob(blob_client, symbol, df, start, end):
    raw_json = df.to_json(orient="records", date_format="iso")
    blob_name = f"backfill/{symbol}_raw_{start}_{end}.json"
    container_client = blob_client.get_container_client("crypto-raw")
    container_client.upload_blob(name=blob_name, data=raw_json, overwrite=True)
    print(f"📂 Raw {symbol} disimpan di Blob: {blob_name}")
    return blob_name

# ===============================
# INSERT BATCH
# ===============================

def insert_curated(cursor, df, symbol, batch_size=500):
    df['date'] = df['Datetime'].dt.date
    df['hourx'] = df['Datetime'].dt.hour
    df['crypto'] = symbol
    curated = df[['date','hourx','crypto','Open','High','Low','Close','Volume']]

    # --- Log bulanan ---
    df['month'] = pd.to_datetime(df['date']).dt.to_period('M')
    monthly_counts = df.groupby('month').size()
    print(f"📅 Ringkasan {symbol}:")
    for m, count in monthly_counts.items():
        print(f"   {m}: {count} baris")

    inserted_total = 0
    for start in range(0, len(curated), batch_size):
        batch = curated.iloc[start:start+batch_size]
        values = []

        for _, row in batch.iterrows():
            date_val   = to_scalar(row['date'], cast_type=None)
            hourx_val  = to_scalar(row['hourx'], cast_type=int)
            open_val   = to_scalar(row['Open'], cast_type=float)
            high_val   = to_scalar(row['High'], cast_type=float)
            low_val    = to_scalar(row['Low'], cast_type=float)
            close_val  = to_scalar(row['Close'], cast_type=float)
            volume_val = to_scalar(row['Volume'], cast_type=int)

            values.append((date_val, hourx_val, str(symbol),
                           open_val, high_val, low_val, close_val, volume_val,
                           date_val, hourx_val, str(symbol)))  # untuk WHERE NOT EXISTS

        query = """
        INSERT INTO CryptoPrice (date,hourx,crypto,[Open],[High],[Low],[Close],[Volume])
        SELECT ?,?,?,?,?,?,?,?
        WHERE NOT EXISTS (
            SELECT 1 FROM CryptoPrice WHERE date=? AND hourx=? AND crypto=?
        )
        """
        cursor.executemany(query, values)
        inserted_total += cursor.rowcount

    print(f"📊 {symbol}: {inserted_total} baris baru ditambahkan (duplikat otomatis diskip)")

# ===============================
# MAIN
# ===============================

def main():
    print("🚀 START BACKFILL")
    blob_client = connect_blob()
    conn = connect_sql()
    cursor = conn.cursor()
    create_tables(cursor)

    for symbol in CRYPTOS:
        df = fetch_data(symbol, START, END, INTERVAL)
        save_raw_to_blob(blob_client, symbol, df, START, END)
        insert_curated(cursor, df, symbol, batch_size=500)
        conn.commit()

    cursor.close()
    conn.close()
    print("✅ BACKFILL SELESAI!")

if __name__ == "__main__":
    main()
