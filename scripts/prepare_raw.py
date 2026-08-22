import duckdb
from pathlib import Path
import shutil
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

PROJECT_DIR = Path.cwd()
LOCAL_SOURCE_PATH = PROJECT_DIR / "data" / "raw" / "multi_event.parquet"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

CLEAN_PATH = PROCESSED_DIR / "multi_event_clean.parquet"
TMP_DIR = PROCESSED_DIR / "tmp_duckdb"
TMP_DIR.mkdir(parents=True, exist_ok=True)

if not LOCAL_SOURCE_PATH.is_file():
    logging.info("Dataset not found locally. Downloading from HuggingFace...")
    from huggingface_hub import hf_hub_download
    cached = hf_hub_download(
        repo_id="yandex/yambda",
        filename="flat/500m/multi_event.parquet",
        repo_type="dataset",
    )
    LOCAL_SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(cached, LOCAL_SOURCE_PATH)
    logging.info("Dataset downloaded successfully.")

logging.info("Connecting to DuckDB for out-of-core processing...")
con = duckdb.connect(database=':memory:')

# Force DuckDB to use disk for temp storage and limit memory to avoid OOM
con.execute("PRAGMA memory_limit='4GB'")
con.execute(f"PRAGMA temp_directory='{TMP_DIR.as_posix()}'")

query = f"""
COPY (
    WITH raw_data AS (
        SELECT *,
               CAST(timestamp // 86400 AS UINTEGER) AS time_period,
               (event_type = 'listen') AS is_listen
        FROM read_parquet('{LOCAL_SOURCE_PATH.as_posix()}')
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
) TO '{CLEAN_PATH.as_posix()}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
"""

logging.info("Executing DuckDB pipeline (spilling to disk). This may take 5-10 minutes...")
con.execute(query)

logging.info(f"Finished successfully! Clean dataset saved to {CLEAN_PATH}")
# Clean up temp dir
shutil.rmtree(TMP_DIR)
