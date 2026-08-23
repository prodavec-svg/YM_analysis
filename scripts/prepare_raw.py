import duckdb
from pathlib import Path
import shutil
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_GLOB = PROJECT_DIR / "data" / "raw" / "raw_split_15days" / "**" / "*.parquet"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
CLEAN_DIR = PROCESSED_DIR / "multi_event_clean"
TMP_DIR = PROCESSED_DIR / "tmp_duckdb"

def parse_args():
    parser = argparse.ArgumentParser(description="Clean raw partitioned data.")
    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT_GLOB), help="Glob pattern for input chunks")
    return parser.parse_args()

def main():
    args = parse_args()
    
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    logging.info(f"Connecting to DuckDB. Reading from: {args.input}")
    con = duckdb.connect(database=':memory:')
    con.execute("PRAGMA memory_limit='8GB'")
    con.execute(f"PRAGMA temp_directory='{TMP_DIR.as_posix()}'")

    # Убеждаемся, что мы не дописываем в старые данные, если перезапускаем скрипт
    shutil.rmtree(CLEAN_DIR, ignore_errors=True)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    query = f"""
    COPY (
        WITH raw_data AS (
            SELECT *,
                   CAST(timestamp // 86400 AS UINTEGER) AS time_period,
                   (event_type = 'listen') AS is_listen
            FROM read_parquet('{args.input}')
            WHERE timestamp IS NOT NULL AND uid IS NOT NULL
        ),
        dedup AS (
            SELECT DISTINCT ON (uid, timestamp, item_id, event_type) *
            FROM raw_data
        ),
        velocity AS (
            SELECT uid, timestamp,
                   count(*) as events_in_5s
            FROM dedup
            GROUP BY uid, timestamp
        ),
        flags AS (
            SELECT uid, timestamp,
                   (events_in_5s > 15) AS is_bot_session,
                   (events_in_5s > 5 AND events_in_5s <= 15) AS is_offline_sync
            FROM velocity
        ),
        bot_users AS (
            SELECT DISTINCT uid
            FROM flags
            WHERE is_bot_session = true
        )
        SELECT d.uid,
               d.timestamp,
               d.item_id,
               d.is_organic,
               d.played_ratio_pct,
               d.track_length_seconds,
               d.event_type,
               d.time_period,
               d.is_listen,
               
               CASE WHEN d.is_listen AND d.played_ratio_pct > 100 THEN true ELSE false END AS is_replayed_over_100pct,
               CASE WHEN d.is_listen AND d.track_length_seconds > 405.0 THEN true ELSE false END AS is_long_content,
               
               CASE WHEN d.is_listen THEN d.track_length_seconds * d.played_ratio_pct / 100.0 ELSE NULL END AS played_seconds_raw,
               CASE WHEN d.is_listen THEN d.track_length_seconds * LEAST(GREATEST(d.played_ratio_pct, 0), 100) / 100.0 ELSE NULL END AS played_seconds_capped,
               
               CASE WHEN b.uid IS NOT NULL THEN true ELSE false END AS is_suspected_bot_user,
               
               COALESCE(f.is_bot_session, false) AS is_bot_session,
               COALESCE(f.is_offline_sync, false) AS is_offline_sync,
               
               (COALESCE(f.is_bot_session, false) = false AND COALESCE(f.is_offline_sync, false) = false) AS sequence_eligible
               
        FROM dedup d
        LEFT JOIN flags f ON d.uid = f.uid AND d.timestamp = f.timestamp
        LEFT JOIN bot_users b ON d.uid = b.uid
        
        WHERE COALESCE(f.is_bot_session, false) = false
    ) TO '{CLEAN_DIR.as_posix()}' (FORMAT PARQUET, PARTITION_BY (time_period), COMPRESSION 'ZSTD', OVERWRITE_OR_IGNORE);
    """

    logging.info("Cleaning data and writing partitioned output by DAY. This may take 15-30 minutes per colleague...")
    con.execute(query)

    logging.info(f"Clean dataset saved to {CLEAN_DIR} (partitioned by day)")
    shutil.rmtree(TMP_DIR, ignore_errors=True)

if __name__ == '__main__':
    main()
