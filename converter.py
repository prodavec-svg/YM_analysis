import pandas as pd
from pathlib import Path

# Папки
MARTS_DIR = Path("data/marts")
CSV_DIR = Path("data/saved_csv")

# Создаём папку для CSV, если её нет
CSV_DIR.mkdir(parents=True, exist_ok=True)

# Находим все parquet-файлы
parquet_files = list(MARTS_DIR.glob("*.parquet"))

if not parquet_files:
    print(f"Нет .parquet файлов в {MARTS_DIR}")
else:
    for parquet_path in parquet_files:
        # Читаем
        df = pd.read_parquet(parquet_path)
        # Имя файла без расширения
        stem = parquet_path.stem
        csv_path = CSV_DIR / f"{stem}.csv"
        # Сохраняем
        df.to_csv(csv_path, index=False)
        print(f"Конвертирован: {parquet_path.name} -> {csv_path.name}")

print("Готово.")