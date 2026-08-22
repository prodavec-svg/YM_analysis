import polars as pl
from pathlib import Path
import shutil

PROJECT_DIR = Path.cwd()
LOCAL_SOURCE_PATH = PROJECT_DIR / "data" / "raw" / "multi_event.parquet"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

CLEAN_PATH = PROCESSED_DIR / "multi_event_clean.parquet"
VELOCITY_BINS_PATH = PROCESSED_DIR / "multi_event_velocity_bins.parquet"

EXPECTED_COLUMNS = [
    "uid", "timestamp", "item_id", "is_organic",
    "played_ratio_pct", "track_length_seconds", "event_type",
]
BOT_BIN_THRESHOLD = 15
OFFLINE_SYNC_THRESHOLD = 5

if not LOCAL_SOURCE_PATH.is_file():
    from huggingface_hub import hf_hub_download
    cached = hf_hub_download(
        repo_id="yandex/yambda",
        filename="flat/500m/multi_event.parquet",
        repo_type="dataset",
    )
    LOCAL_SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(cached, LOCAL_SOURCE_PATH)

print("Reading dataset...")
events_lf = pl.scan_parquet(LOCAL_SOURCE_PATH)

# Basic derived columns
analysis_lf = events_lf.with_columns(
    (pl.col("timestamp").cast(pl.UInt64) // 86400).cast(pl.UInt32).alias("time_period"),
    (pl.col("event_type") == "listen").alias("is_listen"),
)

print("Deduplicating...")
valid_deduplicated_lf = analysis_lf.filter(
    pl.col("timestamp").is_not_null() & pl.col("uid").is_not_null()
).unique(subset=["uid", "timestamp", "item_id", "event_type"])

print("Computing velocity bins...")
velocity_flags_lf = (
    valid_deduplicated_lf.group_by(["uid", "timestamp"])
    .agg(
        pl.len().alias("events_in_5s"),
        pl.col("item_id").n_unique().alias("unique_items_in_5s"),
        pl.col("event_type").n_unique().alias("event_types_in_5s"),
    )
    .with_columns(
        (pl.col("events_in_5s") > BOT_BIN_THRESHOLD).alias("is_bot_session"),
        pl.col("events_in_5s").is_between(
            OFFLINE_SYNC_THRESHOLD + 1, BOT_BIN_THRESHOLD, closed="both"
        ).alias("is_offline_sync"),
    )
)
velocity_flags_lf.sink_parquet(VELOCITY_BINS_PATH, compression="zstd", mkdir=True)

print("Rejoining and calculating final fields...")
velocity_flags_lf = pl.scan_parquet(VELOCITY_BINS_PATH)
suspected_bot_users_lf = (
    velocity_flags_lf.filter(pl.col("is_bot_session"))
    .select("uid").unique()
    .with_columns(pl.lit(True).alias("is_suspected_bot_user"))
)

listen_expr = pl.col("event_type") == "listen"
length_p99 = 405.0 # hardcoded approx from previous run to avoid collect

curated_lf = (
    valid_deduplicated_lf
    .join(velocity_flags_lf, on=["uid", "timestamp"], how="left")
    .join(suspected_bot_users_lf, on="uid", how="left")
    .with_columns(
        pl.col("is_suspected_bot_user").fill_null(False),
        (listen_expr & (pl.col("played_ratio_pct") > 100)).alias("is_replayed_over_100pct"),
        (listen_expr & (pl.col("track_length_seconds") > length_p99)).alias("is_long_content"),
        pl.when(listen_expr)
        .then(pl.col("track_length_seconds").cast(pl.Float64) * pl.col("played_ratio_pct") / 100)
        .otherwise(None).alias("played_seconds_raw"),
        pl.when(listen_expr)
        .then(pl.col("track_length_seconds").cast(pl.Float64) * pl.col("played_ratio_pct").clip(0, 100) / 100)
        .otherwise(None).alias("played_seconds_capped"),
    )
    .with_columns((~pl.col("is_bot_session") & ~pl.col("is_offline_sync")).alias("sequence_eligible"))
)

clean_lf = curated_lf.filter(~pl.col("is_bot_session").fill_null(False))
print("Writing multi_event_clean.parquet...")
clean_lf.sink_parquet(CLEAN_PATH, compression="zstd", mkdir=True)

print("Pipeline finished successfully!")
