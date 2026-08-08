# Yambda data marts

Проект преобразует сырые логи лайков Yambda в три компактные parquet-витрины
для дашборда. Исходник обрабатывается лениво через `pl.scan_parquet()`, а
агрегации выполняются streaming engine Polars, поэтому полный лог не загружается
в память до агрегации.

## Структура

```text
YM_analysis/
├── data/
│   ├── raw/                    # локальные исходники, исключены из Git
│   └── marts/                  # сгенерированные витрины
├── scripts/
│   ├── build_marts.py          # сборка и автоматическая проверка
│   └── validate_marts.py       # отдельный запуск проверок
├── main.ipynb                  # исходный исследовательский ноутбук
├── requirements.txt
└── README.md
```

## Запуск

```powershell
python -m pip install -r requirements.txt
python scripts/build_marts.py --input data/raw/likes.parquet
```

Если `--input` не указан, скрипт последовательно ищет `data/raw/likes.parquet`,
`data/raw/multi_event.parquet` и поддерживаемые файлы в корне проекта. По
умолчанию результаты записываются в `data/marts/`.

Повторная независимая проверка:

```powershell
python scripts/validate_marts.py --input data/raw/likes.parquet
```

## Витрины

- `mart_daily_metrics.parquet`: `time_period`, `dau`,
  `total_interactions`, `organic_ratio`. Один условный день равен 17 280
  пятисекундным бинам (`timestamp // 17280`).
- `mart_user_segments.parquet`: `uid`, `total_likes`, `organic_likes`,
  `algo_likes`, `segment`. Правила применяются в порядке Cold → Explorer →
  Passive → Mixed; граница 70% строгая, как в задании (`> 0.7`).
- `mart_content_health.parquet`: `item_id`, `total_likes`, `organic_ratio`,
  `content_tier`. Треки сортируются по числу лайков, а равенства разрешаются по
  `item_id`; первые `ceil(N × 1%)` получают Head, позиции до
  `ceil(N × 20%)` — Torso, остальные — Tail.

Проверка контролирует отсутствие пропусков, уникальность ключей, ненулевой DAU,
диапазон долей `[0, 1]`, сумму долей сегментов 100%, совпадение количества
пользователей, треков и взаимодействий с исходником, а также лимит 10 МиБ на
каждый файл витрины.

## Визуализация

```powershell
python scripts/build_visualizations.py
```

Команда пересобирает графики и аналитическую сводку в `reports/` напрямую из
трёх свежих parquet-витрин. Готовая демонстрация находится в
[`reports/README.md`](reports/README.md).

## Анализ аномалий и практика Polars

В `main.ipynb` добавлен воспроизводимый учебный раздел на данных Yambda:
ленивое чтение parquet, выражения, фильтрация, агрегации, streaming-вычисления,
поиск аномалий и сохранение результата.

После выполнения раздела создаются локальные файлы:

- `data/processed/likes_clean.parquet` — строки с корректной схемой и без
  полных дубликатов;
- `data/processed/anomaly_summary.parquet` — сводка проверок качества и
  p99-кандидатов.

Содержимое `data/processed/` исключено из Git и воспроизводится из сырого
датасета. Подробное описание всех команд, функций и выходных файлов находится в
[`scripts/README.md`](scripts/README.md).
