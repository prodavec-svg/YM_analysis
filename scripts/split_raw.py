"""
Script to download and logically partition the massive 5B rows dataset.
It uses HuggingFace Hub for robust, resumable downloading.
Then it uses DuckDB to split the 50GB file into smaller 15-day chunks 
so that low-RAM machines can process them sequentially.
"""

import duckdb
import os
from pathlib import Path
import logging
from huggingface_hub import hf_hub_download

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_DIR = Path(__file__).resolve().parents[1]
LOCAL_SOURCE_PATH = PROJECT_DIR / "data" / "raw" / "multi_event_5b.parquet"
OUTPUT_DIR = PROJECT_DIR / "data" / "raw" / "raw_split_15days"

def main():
    # 1. Download the 5B dataset safely
    if not LOCAL_SOURCE_PATH.is_file():
        logging.info("5B Dataset not found locally. Starting robust download from HF...")
        cached = hf_hub_download(
            repo_id="yandex/yambda",
            filename="flat/5b/multi_event.parquet",
            repo_type="dataset",
        )
        LOCAL_SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.symlink(cached, LOCAL_SOURCE_PATH)
        except FileExistsError:
            pass
        logging.info("Download complete!")
    else:
        logging.info("5B Dataset already present!")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 2. Split dataset into 15-day chunks
    logging.info("Connecting to DuckDB to split the dataset...")
    con = duckdb.connect(database=':memory:')
    con.execute("PRAGMA memory_limit='8GB'")
    
    # Formula: (timestamp // 86400) gives days. 
    # (days // 15) gives a chunk ID where each chunk spans exactly 15 days.
    query = f"""
    COPY (
        SELECT *, 
               CAST(timestamp // 86400 AS UINTEGER) AS time_period,
               CAST((timestamp // 86400) // 15 AS UINTEGER) AS chunk_id
        FROM read_parquet('{LOCAL_SOURCE_PATH.as_posix()}')
    ) TO '{OUTPUT_DIR.as_posix()}' (FORMAT PARQUET, PARTITION_BY (chunk_id), COMPRESSION 'ZSTD', OVERWRITE_OR_IGNORE);
    """
    
    logging.info("Splitting into 15-day chunks. This is purely I/O bounded and will take a few minutes...")
    con.execute(query)
    logging.info(f"Done! Check the {OUTPUT_DIR} folder and distribute the chunks to your colleagues.")

if __name__ == '__main__':
    main()
