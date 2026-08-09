# Yambda data marts

Проект преобразует сырые multi-event логи Yambda (`listen`/`like`/`unlike`/
`dislike`/`undislike`) в четыре компактные parquet-витрины для дашборда.
Источник — обогащённый в `main.ipynb` датасет — обрабатывается лениво через
`pl.scan_parquet()`, а агрегации выполняются streaming engine Polars, поэтому
полный лог не загружается в память до агрегации.

## Структура

```text
YM_analysis/
├── data/
│   ├── raw/                    # локальные исходники, исключены из Git
│   ├── processed/              # обогащённые данные и сводка аномалий, исключены из Git
│   └── marts/                  # сгенерированные витрины, исключены из Git
├── scripts/
│   ├── build_marts_v2.py       # сборка витрин из multi_event_enriched.parquet
│   └── validate_marts.py       # отдельный запуск проверок
├── main.ipynb                  # исследовательский ноутбук: EDA, обогащение, аномалии
├── visualization.ipynb         # витринные графики для дашборда
├── requirements.txt
└── README.md
```

## Запуск

```powershell
python -m pip install -r requirements.txt
python scripts/build_marts_v2.py --input data/processed/multi_event_enriched.parquet
```

`multi_event_enriched.parquet` — результат обогащения в `main.ipynb` (см.
раздел ниже); без него собирать витрины нечем. По умолчанию результаты
записываются в `data/marts/`.

## Витрины

Все четыре витрины строятся из одного `LazyFrame` (`scan_source()`), который
уже несёт `time_period`, `is_listen`, `played_ratio_capped_pct` и
`played_seconds_capped`.

- `mart_daily_metrics.parquet` — 1 строка = 1 условный день (`timestamp //
  17 280`). DAU, число активных треков, разбивка событий по типам
  (`listens`, `likes`, `unlikes`, `dislikes`, `undislikes`), часы
  прослушивания, `completion_rate`, `short_listen_rate`,
  `organic_listen_ratio`.
- `mart_event_daily.parquet` — 1 строка = день × тип события × органика/алго.
  Число событий, уникальных пользователей и треков в разрезе.
- `mart_user_segments.parquet` — 1 строка = 1 пользователь. Активность по
  всем типам событий плюс `segment`: Cold (< 5 лайков) → Explorer
  (organic-лайки > 70%) → Passive (algo-лайки > 70%) → Mixed (остальные).
- `mart_content_health.parquet` — 1 строка = 1 трек, встретившийся хотя бы в
  одном событии (не только `like`). Прослушивания, лайки/дизлайки,
  `completion_rate`, `short_listen_rate`, `organic_ratio` и `content_tier`:
  треки сортируются по `total_likes` (равенства — по `item_id`), первые
  `ceil(N × 1%)` → Head, до `ceil(N × 20%)` → Torso, остальные → Tail.

`validate_marts.py` пока сверяет витрины со старой (до multi-event) схемой —
после перехода на `build_marts_v2.py` его нужно обновить под актуальный набор
колонок, иначе проверка будет падать на честном прогоне.

## Анализ аномалий и обогащение (main.ipynb)

`main.ipynb` полностью построен вокруг `data/raw/multi_event.parquet`:
ленивое чтение, EDA, пользовательские/трековые/временные срезы,
визуализации, общие проверки и доменные аномалии playback/state/velocity.

После выполнения ноутбука создаются локальные файлы:

- `data/processed/multi_event_enriched.parquet` — дедуплицированные события с
  расчётным временем прослушивания и anomaly-флагами (вход для
  `build_marts_v2.py`);
- `data/processed/multi_event_row_anomalies.parquet` — строки с playback- или
  length-кандидатами;
- `data/processed/multi_event_anomaly_summary.parquet` — сводка schema,
  playback, state-transition и velocity-проверок.

Содержимое `data/processed/` исключено из Git и воспроизводится из сырого
датасета. Подробное описание всех команд и функций — в
[`scripts/README.md`](scripts/README.md).

## Графики (visualization.ipynb)

`visualization.ipynb` читает готовые витрины из `data/marts/` и строит
витринные графики для дашборда: динамику продукта по неделям, сегменты
пользователей и здоровье каталога по тирам. Витрины нужно собрать заранее
через `build_marts_v2.py` — сам ноутбук их не пересчитывает.
