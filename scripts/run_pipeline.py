import sys
import os
from pathlib import Path
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_DIR = Path(__file__).resolve().parents[1]
RAW_SPLIT_DIR = PROJECT_DIR / "data" / "raw" / "raw_split_15days"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
MARTS_DIR = PROJECT_DIR / "data" / "marts"

def main():
    if not RAW_SPLIT_DIR.exists():
        logging.error(f"Directory {RAW_SPLIT_DIR} not found. Run split_raw.py first.")
        return

    MARTS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Находим все папки с кусками на этом компьютере
    chunks = sorted([d for d in RAW_SPLIT_DIR.iterdir() if d.is_dir() and d.name.startswith("chunk_id=")])
    
    if not chunks:
        logging.info("No chunks found to process.")
        return

    logging.info(f"Found {len(chunks)} chunks to process on this machine.")

    for chunk_dir in chunks:
        chunk_id = chunk_dir.name.split('=')[1]
        logging.info(f"--- Processing {chunk_dir.name} ---")
        
        # 1. Очистка одного куска
        clean_file = PROCESSED_DIR / f"clean_chunk_{chunk_id}.parquet"
        prepare_cmd = [
            sys.executable, str(PROJECT_DIR / "scripts" / "prepare_raw.py"),
            "--input", f"{chunk_dir.as_posix()}/*.parquet",
            "--output", clean_file.as_posix()
        ]
        logging.info(f"Cleaning: {chunk_dir.name} -> {clean_file.name}")
        subprocess.run(prepare_cmd, check=True)
        
        # 2. Сборка витрины из этого очищенного куска
        mart_file = MARTS_DIR / f"mart_unified_chunk_{chunk_id}.parquet"
        build_cmd = [
            sys.executable, str(PROJECT_DIR / "scripts" / "build_marts.py"),
            "--input", clean_file.as_posix(),
            "--marts-dir", MARTS_DIR.as_posix()
        ]
        # Костыль: build_marts.py сохраняет файл как mart_unified.parquet, 
        # поэтому мы переименуем его после завершения скрипта, чтобы куски не перезаписывали друг друга
        logging.info(f"Building mart: {clean_file.name} -> {mart_file.name}")
        subprocess.run(build_cmd, check=True)
        
        # Переименовываем финальный файл
        default_mart_path = MARTS_DIR / "mart_unified.parquet"
        if default_mart_path.exists():
            default_mart_path.rename(mart_file)
            
        logging.info(f"Finished {chunk_dir.name}!\n")

    logging.info("All chunks processed successfully! Send the files from data/marts/ to the main assembler.")

if __name__ == '__main__':
    main()
