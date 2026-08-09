# Документация Python-скриптов

В папке `scripts/` находятся два исполняемых модуля ETL-контура Yambda:

1. `build_marts_v2.py` читает обогащённый multi-event parquet и собирает
   четыре витрины.
2. `validate_marts.py` проверяет витрины и сверяет их с исходником.

Все пути вычисляются относительно корня репозитория, поэтому команды можно
запускать из корня `YM_analysis` независимо от абсолютного расположения клона.

## Общая схема работы

```text
data/raw/multi_event.parquet
        │
        ▼
main.ipynb (обогащение, anomaly-флаги)
        │
        ▼
data/processed/multi_event_enriched.parquet
        │
        ▼
scripts/build_marts_v2.py
        │
        ├── data/marts/mart_daily_metrics.parquet
        ├── data/marts/mart_event_daily.parquet
        ├── data/marts/mart_user_segments.parquet
        └── data/marts/mart_content_health.parquet
                         │
                         ▼
              scripts/validate_marts.py
```

Установка зависимостей:

```powershell
python -m pip install -r requirements.txt
```

Рекомендуемый полный запуск:

```powershell
python scripts/build_marts_v2.py --input data/processed/multi_event_enriched.parquet
python scripts/validate_marts.py --input data/raw/multi_event.parquet
```

> `validate_marts.py` пока проверяет старую (до multi-event) схему витрин —
> `EXPECTED_COLUMNS` и business-инварианты нужно обновить под колонки,
> которые реально пишет `build_marts_v2.py` (см. ниже), иначе проверка будет
> падать на честном прогоне.

---

## `build_marts_v2.py`

### Назначение

Скрипт реализует ETL-пайплайн на ленивом API Polars. Источник — уже
обогащённый в `main.ipynb` `multi_event_enriched.parquet` — открывается через
`pl.scan_parquet()`, преобразования формируются как `LazyFrame`, а запись идёт
через `sink_parquet()` (streaming). Перед заменой целевого файла готовая
витрина записывается во временный parquet, поэтому незавершённый расчёт не
повреждает предыдущий результат.

### Интерфейс командной строки

```powershell
python scripts/build_marts_v2.py `
  --input data/processed/multi_event_enriched.parquet `
  --output-dir data/marts
```

| Аргумент | Обязательный | Значение |
|---|---|---|
| `--input PATH` | Нет | Обогащённый multi-event parquet. По умолчанию `data/processed/multi_event_enriched.parquet`. |
| `--output-dir PATH` | Нет | Каталог витрин. По умолчанию `data/marts/`. |

### Ожидаемая схема входа

| Поле | Назначение |
|---|---|
| `uid` | Идентификатор пользователя. |
| `item_id` | Идентификатор трека. |
| `timestamp` | Номер пятисекундного временного бина. |
| `is_organic` | `1` — самостоятельное обнаружение, `0` — рекомендация. |
| `event_type` | Один из `listen`, `like`, `unlike`, `dislike`, `undislike`. |
| `played_ratio_pct` | Доля прослушанного трека, % (только для `listen`). |
| `track_length_seconds` | Длина трека, сек. |

`scan_source()` проверяет наличие обязательных полей, фильтрует строки с
некорректными `event_type`/`is_organic`, считает `time_period` (`timestamp //
17 280`), `is_listen`, `played_ratio_capped_pct` (клип 0–100) и
`played_seconds_capped`.

### Формируемые витрины

#### `mart_daily_metrics.parquet`

Зерно: один условный день. Содержит DAU, число активных треков, разбивку
событий по типам (`listens`, `likes`, `unlikes`, `dislikes`, `undislikes`),
`completed_listens`/`short_listens` (по порогам 90%/10% дослушивания),
`played_hours`, `avg_played_ratio_pct` и производные доли
(`completion_rate`, `short_listen_rate`, `organic_listen_ratio`).

#### `mart_event_daily.parquet`

Зерно: день × `event_type` × `is_organic`. Число событий, уникальных
пользователей и треков в каждом разрезе — для графиков динамики по типам
событий.

#### `mart_user_segments.parquet`

Зерно: один пользователь. Активность по всем типам событий плюс
`total_likes`/`organic_likes`/`algo_likes` и `segment`:

1. `Cold`: меньше пяти лайков.
2. `Explorer`: доля органических лайков строго больше 70%.
3. `Passive`: доля алгоритмических лайков строго больше 70%.
4. `Mixed`: все остальные пользователи.

#### `mart_content_health.parquet`

Зерно: один трек, встретившийся хотя бы в одном событии (не только `like`).
Прослушивания, лайки/дизлайки, `completion_rate`, `short_listen_rate`,
`organic_ratio`, `track_length_seconds` и `content_tier`. После агрегации
треки сортируются по `total_likes` по убыванию (равенства — по `item_id`):

- `Head`: первые `ceil(N × 1%)` треков;
- `Torso`: позиции после Head до `ceil(N × 20%)`;
- `Tail`: оставшиеся 80% каталога.

Поскольку зерно — весь каталог (а не только лайкнутые треки), эта витрина на
порядок больше остальных.

### Функции

| Функция | Ответственность |
|---|---|
| `parse_args()` | Разбирает CLI-аргументы. |
| `scan_source()` | Проверяет схему, фильтрует и обогащает `LazyFrame` (`time_period`, `is_listen`, playback-поля). |
| `mart_daily_metrics()` | Строит ленивый план дневной витрины. |
| `mart_event_daily()` | Строит витрину день × тип события × органика. |
| `mart_user_segments()` | Строит пользовательские агрегаты и сегменты. |
| `mart_content_health()` | Считает популярность, качество прослушивания и tier трека. |
| `write_mart()` | Выполняет streaming-запись и атомарно заменяет parquet с Zstandard-сжатием. |
| `main()` | Координирует полный ETL. |

---

## `validate_marts.py`

### Назначение

Скрипт выполняет независимые проверки файлов и бизнес-инвариантов. При любой
ошибке он завершает работу с ненулевым кодом и понятным исключением. При
успехе печатает основные объёмы, доли сегментов и размеры файлов.

Запуск:

```powershell
python scripts/validate_marts.py `
  --input data/raw/multi_event.parquet `
  --marts-dir data/marts
```

| Аргумент | Обязательный | Значение |
|---|---|---|
| `--input PATH` | Да | Исходный parquet для перекрёстной сверки. |
| `--marts-dir PATH` | Нет | Каталог витрин; по умолчанию `data/marts/`. |

### Проверки (актуальны для старой схемы, требуют обновления под v2)

- наличие всех parquet-файлов;
- точное совпадение обязательных столбцов (`EXPECTED_COLUMNS`);
- размер каждого файла не более 10 МиБ;
- отсутствие пропусков в обязательных core-полях;
- бинарность `is_organic`;
- уникальность ключей `time_period`, `uid`, `item_id`;
- положительный DAU;
- диапазон всех долей от 0 до 1;
- совпадение числа пользователей и треков с исходником;
- равенство `organic_likes + algo_likes = total_likes`;
- правильность сегмента каждой пользовательской строки;
- сумма долей пользовательских сегментов 100%;
- точное соответствие размеров Head, Torso и Tail заданным процентилям.

Известные расхождения с `build_marts_v2.py` (нужно поправить перед следующим
прогоном): `EXPECTED_COLUMNS` перечисляет старые 4-колоночные схемы, а не
реальные ~16 колонок каждой витрины; `mart_event_daily.parquet` вообще не
охвачен; лимит 10 МиБ для `mart_content_health.parquet` не проходит, так как
зерно витрины расширилось до всего каталога (а не только лайкнутых треков).

### Функции

| Функция | Ответственность |
|---|---|
| `_collect()` | Выполняет ленивый план streaming-движком. |
| `_scalar()` | Безопасно извлекает скаляр из результата проверки. |
| `_check_files()` | Проверяет наличие, размер и схему файлов. |
| `validate_marts()` | Выполняет полный набор проверок и возвращает словарь метрик. |
| `parse_args()` | Разбирает CLI-аргументы автономного запуска. |

Функцию `validate_marts()` можно импортировать из другого Python-кода. При
`verbose=False` она не печатает отчёт, но по-прежнему выбрасывает исключение
при нарушении инварианта.

---

## Воспроизводимость и диагностика

- Скрипты рассчитаны на Python 3.10+ и версии библиотек из `requirements.txt`.
- Сырой, очищенный датасеты и витрины находятся вне Git — все они
  воспроизводятся из `data/raw/multi_event.parquet` через `main.ipynb` и
  `build_marts_v2.py`.
- Для просмотра плана Polars перед выполнением можно вызвать `plan.explain()`.
- Если сборка завершилась, но проверка упала, не используйте
  `--skip-validation`-подобные обходы как постоянное решение: сначала
  устраните нарушенный инвариант (или обновите сам инвариант под новую
  схему — см. раздел `validate_marts.py` выше).
- После изменения правил витрин запускайте обе команды полного цикла и
  проверяйте `git diff --check` перед коммитом.
