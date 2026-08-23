import duckdb
from pathlib import Path
import logging
from huggingface_hub import hf_hub_download
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_DIR = Path(__file__).resolve().parents[1]
LOCAL_SOURCE_PATH = PROJECT_DIR / "data" / "raw" / "multi_event_5b.parquet"
OUTPUT_DIR = PROJECT_DIR / "data" / "raw" / "raw_split_15days"

def main():
    if not LOCAL_SOURCE_PATH.is_file():
        logging.info("5B Dataset not found locally. Starting robust download from HF...")
        cached = hf_hub_download(
            repo_id="yandex/yambda",
            filename="flat/5b/multi_event.parquet",
            repo_type="dataset",
            # resume_download=True  # happens automatically in new versions
        )
        LOCAL_SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.symlink(cached, LOCAL_SOURCE_PATH)
        except FileExistsError:
            pass
        logging.info("Download complete!")
    else:
        logging.info("5B Dataset already downloaded!")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    logging.info("Connecting to DuckDB to split the dataset...")
    con = duckdb.connect(database=':memory:')
    con.execute("PRAGMA memory_limit='8GB'")
    
    # Мы делим timestamp на 86400 чтобы получить дни (time_period).
    # А чтобы в одном файле было 15 дней, мы делим time_period на 15.
    # Так 0-14 дни попадут в chunk=0, 15-29 в chunk=1 и т.д.
    query = f"""
    COPY (
        SELECT *, 
               CAST(timestamp // 86400 AS UINTEGER) AS time_period,
               CAST((timestamp // 86400) // 15 AS UINTEGER) AS chunk_id
        FROM read_parquet('{LOCAL_SOURCE_PATH.as_posix()}')
    ) TO '{OUTPUT_DIR.as_posix()}' (FORMAT PARQUET, PARTITION_BY (chunk_id), COMPRESSION 'ZSTD', OVERWRITE_OR_IGNORE);
    """
    
    logging.info("Splitting into 15-day chunks. This will take a while, but it's purely I/O bounded...")
    con.execute(query)
    logging.info(f"Done! Check the {OUTPUT_DIR} folder.")

if __name__ == '__main__':
    main()
