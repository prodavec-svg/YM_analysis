# Yambda data marts

Проект преобразует сырые multi-event логи Yambda (`listen`/`like`/`unlike`/
`dislike`/`undislike`) в пять компактных parquet-витрин для дашборда.
Источник — очищенный в `main.ipynb` датасет (без точных дублей, invalid-строк
и bot-бинов) — обрабатывается лениво через `pl.scan_parquet()`, а агрегации
выполняются streaming engine Polars, поэтому полный лог не загружается в
память до агрегации.

## Структура

```text
YM_analysis/
├── data/
│   ├── raw/                    # локальные исходники, исключены из Git
│   ├── processed/              # обогащённые данные и сводка аномалий, исключены из Git
│   └── marts/                  # сгенерированные витрины, исключены из Git
├── scripts/
│   ├── build_marts_v2.py       # сборка витрин из multi_event_clean.parquet
│   └── validate_marts.py       # отдельный запуск проверок
├── main.ipynb                  # исследовательский ноутбук: EDA, обогащение, аномалии
├── visualization.ipynb         # витринные графики для дашборда
├── requirements.txt
└── README.md
```

## Запуск

```powershell
python -m pip install -r requirements.txt
python scripts/build_marts_v2.py --input data/processed/multi_event_clean.parquet
python scripts/validate_marts.py --input data/processed/multi_event_clean.parquet
```

Автотесты порогов, bot/sequence-фильтров, сегментации и CSV-экспорта:

```powershell
python -m unittest discover -v
```

`multi_event_clean.parquet` — результат очистки в `main.ipynb` (см. раздел
ниже): без точных дублей, invalid-строк и bot-бинов. Без него собирать
витрины нечем. По умолчанию результаты записываются в `data/marts/`.

## Витрины

Все пять витрин строятся из одного `LazyFrame` (`scan_source()`). Метрики
прослушивания учитывают только `is_listen = true`, `sequence_eligible = true`,
`is_bot_session = false`, `is_suspected_bot_user = false`. Дослушивание —
не менее 80%; short/skip — не более 30 секунд или менее 10% трека.

- `mart_daily_metrics.parquet` — 1 строка = 1 условный день (`timestamp //
  17 280`). DAU, число активных треков, разбивка событий по типам
  (`listens`, `likes`, `unlikes`, `dislikes`, `undislikes`), часы
  прослушивания, `completion_rate`, `short_listen_rate`,
  `organic_listen_ratio`.
- `mart_event_daily.parquet` — 1 строка = день × тип события × органика/алго.
  Число событий, уникальных пользователей и треков в разрезе.
- `mart_user_segments.parquet` — 1 строка = пользователь × период, включая
  пролонгацию сегмента в периоды без активности. Накопительная сегментация:
  Cold (< 5 лайков) → Explorer
  (organic-лайки > 70%) → Passive (algo-лайки > 70%) → Mixed (остальные).
- `mart_content_health.parquet` — 1 строка = 1 трек, встретившийся хотя бы в
  одном событии (не только `like`). Прослушивания, лайки/дизлайки,
  отдельные Algo/Organic completion и skip, `completion_rate`,
  `short_listen_rate`, `organic_ratio` и `content_tier`:
  треки сортируются по `total_likes` (равенства — по `item_id`), первые
  `ceil(N × 1%)` → Head, до `ceil(N × 20%)` → Torso, остальные → Tail.
- `mart_user_general.parquet` — 1 строка = пользователь × период. Содержит
  реакции, число прослушиваний, Algo/Organic completion и skip, а также
  число уникальных треков по каждому источнику. Для файловых источников
  DataLens одновременно экспортируются `mart_user_general.csv`,
  `mart_user_segments.csv` и `mart_content_health.csv` в `data/saved_csv/`.

`validate_marts.py` проверяет схемы, зерно, размеры тиров, полноту пользователей
и треков, а также независимо пересчитывает глобальные Algo/Organic listening,
completion и skip из clean-слоя.

Поля, связи, настройки и приёмка шести чартов DataLens описаны в
[`docs/datalens_stage2.md`](docs/datalens_stage2.md).

## Анализ аномалий, очистка и feature-слой (main.ipynb)

`main.ipynb` полностью построен вокруг `data/raw/multi_event.parquet` и
строит пайплайн `raw → enriched (флаги, ничего не удаляется) → clean (боты и
invalid-строки вырезаны) → нормализованные state-переходы и snapshots`.

После выполнения ноутбука создаются локальные файлы:

- `data/processed/multi_event_enriched.parquet` — дедуплицированные события с
  расчётным временем прослушивания и anomaly-флагами (ничего не удалено, все
  кандидаты помечены);
- `data/processed/multi_event_row_anomalies.parquet` — строки с playback- или
  length-кандидатами;
- `data/processed/multi_event_anomaly_summary.parquet` — сводка schema,
  playback, state-transition и velocity-проверок;
- `data/processed/multi_event_clean.parquet` — очищенный слой (без точных
  дублей, invalid-строк и bot-бинов; **вход для `build_marts_v2.py`**);
- `data/processed/multi_event_velocity_bins.parquet` — профиль 5-секундных
  бинов активности (bot-session / offline-sync флаги);
- `data/processed/multi_event_cleaning_summary.parquet` — сводка по очистке
  (сколько строк удалено на каждом шаге);
- `data/processed/state_transitions_clean.parquet` — нормализованный журнал
  like/dislike-переходов (идемпотентный, с синтетическими начальными
  состояниями);
- `data/processed/state_snapshot_daily_sparse.parquet` — разреженный
  end-of-day snapshot состояния;
- `data/processed/state_snapshot_latest.parquet` — актуальные `is_liked` /
  `is_disliked` на конец окна наблюдения.

Содержимое `data/processed/` исключено из Git и воспроизводится из сырого
датасета. Подробное описание всех команд и функций — в
[`scripts/README.md`](scripts/README.md).

## Графики (visualization.ipynb)

`visualization.ipynb` читает готовые витрины из `data/marts/` и строит
витринные графики для дашборда: динамику продукта по неделям, сегменты
пользователей и здоровье каталога по тирам. Витрины нужно собрать заранее
через `build_marts_v2.py` — сам ноутбук их не пересчитывает.
