import datetime, logging
import azure.functions as func
from utils.data_fetcher import fetch_data
from utils.blob_handler import connect_blob, save_raw_to_blob
from utils.db_handler import connect_sql, insert_incremental
from dotenv import load_dotenv

load_dotenv()

CRYPTOS = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD"]
INTERVAL = "1h"


def main(timer: func.TimerRequest = None) -> None:
    utc_now = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    logging.info(f"===== TimerCryptoIngest started at {utc_now} =====")

    blob_client = connect_blob()
    conn = connect_sql()
    cursor = conn.cursor()

    for symbol in CRYPTOS:
        # ambil last timestamp dari SQL
        cursor.execute("""
            SELECT MAX(DATEADD(HOUR, hourx, CAST(date AS DATETIME2)))
            FROM CryptoPrice WHERE crypto=?
        """, symbol)
        row = cursor.fetchone()
        last_ts = row[0] if row and row[0] else datetime.datetime(2024,1,1)

        logging.info(f"📍 {symbol} last success = {last_ts}")

        # ✅ Guard clause kalau last_ts lebih baru dari waktu sekarang
        if last_ts >= utc_now:
            logging.info(f"⏩ Skip {symbol}: last_ts {last_ts} >= now {utc_now}")
            continue

        # ambil semua data dari last_ts+1 jam sampai sekarang
        start = last_ts + datetime.timedelta(hours=1)
        end = utc_now

        logging.info(f"🔄 Fetching {symbol} from {start} to {end}")
        df = fetch_data(symbol, start, end, INTERVAL)

        if df.empty:
            logging.warning(f"⚠️ No data for {symbol} in {start} - {end}")
        else:
            save_raw_to_blob(blob_client, symbol, df, "incremental")
            insert_incremental(cursor, df, symbol)
            conn.commit()

            # log jam yang berhasil diambil
            jam_list = df[['date','hourx']].drop_duplicates().sort_values(['date','hourx'])
            logging.info(f"✅ {symbol}: {len(jam_list)} rows inserted "
                         f"(hours: {jam_list['hourx'].tolist()})")

    cursor.close()
    conn.close()
    logging.info("All cryptos incremental load finished")


# ✅ supaya bisa dijalankan manual
if __name__ == "__main__":
    main()