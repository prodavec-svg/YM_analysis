# Документация Python-скриптов

В папке `scripts/` находятся два исполняемых модуля ETL-контура Yambda:

1. `build_marts_v2.py` читает обогащённый multi-event parquet и собирает
   пять Parquet-витрин.
2. `validate_marts.py` проверяет витрины и сверяет их с исходником.

Все пути вычисляются относительно корня репозитория, поэтому команды можно
запускать из корня `YM_analysis` независимо от абсолютного расположения клона.

## Общая схема работы

```text
data/raw/multi_event.parquet
        │
        ▼
main.ipynb (обогащение → очистка → state-слой)
        │
        ▼
data/processed/multi_event_clean.parquet
        │
        ▼
scripts/build_marts_v2.py
        │
        ├── data/marts/mart_daily_metrics.parquet
        ├── data/marts/mart_event_daily.parquet
        ├── data/marts/mart_user_segments.parquet
        ├── data/marts/mart_content_health.parquet
        └── data/marts/mart_user_general.parquet
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
python scripts/build_marts_v2.py
python scripts/validate_marts.py --input data/processed/multi_event_clean.parquet
```

---

## `build_marts_v2.py`

### Назначение

Скрипт реализует ETL-пайплайн на ленивом API Polars. Источник — очищенный в
`main.ipynb` `multi_event_clean.parquet` (без точных дублей, invalid-строк и
bot-бинов) — открывается через `pl.scan_parquet()`, преобразования
формируются как `LazyFrame`, а запись идёт через `sink_parquet()`
(streaming). Перед заменой целевого файла готовая витрина записывается во
временный parquet, поэтому незавершённый расчёт не повреждает предыдущий
результат.

### Интерфейс командной строки

```powershell
python scripts/build_marts_v2.py `
  --input data/processed/multi_event_clean.parquet `
  --marts-dir data/marts
```

### Ожидаемая схема входа

| Поле | Назначение |
|---|---|
| `uid` | Идентификатор пользователя. |
| `item_id` | Идентификатор трека. |
| `time_period` | Период, рассчитанный в clean-слое. |
| `is_organic` | `1` — самостоятельное обнаружение, `0` — рекомендация. |
| `event_type` | Один из `listen`, `like`, `unlike`, `dislike`, `undislike`. |
| `played_ratio_pct` | Доля прослушанного трека, % (только для `listen`). |
| `track_length_seconds` | Длина трека, сек. |
| `played_seconds_capped` | Время прослушивания с ограничением длиной трека. |
| `is_listen` | Флаг события прослушивания. |
| `sequence_eligible` | Флаг допустимости для последовательностных метрик. |
| `is_bot_session` | Флаг bot-сессии. |
| `is_suspected_bot_user` | Флаг подозрительного пользователя. |

`scan_source()` проверяет наличие обязательных полей clean-слоя и фильтрует
строки с некорректными `event_type`/`is_organic`. Для listening-метрик единая
маска требует `is_listen`, `sequence_eligible`, отсутствие bot-session и
suspected-bot-user. Источник хранит `played_ratio_pct` в шкале 0–100, поэтому
пороги ТЗ 0.8/0.1 реализованы как 80%/10%; short дополнительно включает
`played_seconds_capped <= 30`.

### Формируемые витрины

#### `mart_daily_metrics.parquet`

Зерно: один условный день. Содержит DAU, число активных треков, разбивку
событий по типам (`listens`, `likes`, `unlikes`, `dislikes`, `undislikes`),
`completed_listens`/`short_listens` (по порогам 80% и 30 сек. или 10%),
`played_hours`, `avg_played_ratio_pct` и производные доли
(`completion_rate`, `short_listen_rate`, `organic_listen_ratio`).

#### `mart_event_daily.parquet`

Зерно: день × `event_type` × `is_organic`. Число событий, уникальных
пользователей и треков в каждом разрезе — для графиков динамики по типам
событий.

#### `mart_user_segments.parquet`

Зерно: `time_period × uid`. Сегмент рассчитывается по накопленным
`total_likes`/`organic_likes`/`algo_likes` и пролонгируется в неактивные дни:

1. `Cold`: меньше пяти лайков.
2. `Explorer`: доля органических лайков строго больше 70%.
3. `Passive`: доля алгоритмических лайков строго больше 70%.
4. `Mixed`: все остальные пользователи.

#### `mart_content_health.parquet`

Зерно: один трек, встретившийся хотя бы в одном событии (не только `like`).
Прослушивания, лайки/дизлайки, `completion_rate`, `short_listen_rate`,
`organic_ratio`, отдельные Algo/Organic completion и skip,
`track_length_seconds` и `content_tier`. После агрегации
треки сортируются по `total_likes` по убыванию (равенства — по `item_id`):

- `Head`: первые `ceil(N × 1%)` треков;
- `Torso`: позиции после Head до `ceil(N × 20%)`;
- `Tail`: оставшиеся 80% каталога.

Поскольку зерно — весь каталог (а не только лайкнутые треки), эта витрина на
порядок больше остальных.

#### `mart_user_general.parquet`

Зерно: `time_period × uid`. Помимо реакций и числа прослушиваний по источникам
содержит шесть новых полей: Algo/Organic completion, Algo/Organic short/skip и
Algo/Organic unique items.

### Функции

| Функция | Ответственность |
|---|---|
| `parse_args()` | Разбирает CLI-аргументы. |
| `scan_source()` | Проверяет схему clean-слоя и типы ключевых полей. |
| `mart_daily_metrics()` | Строит ленивый план дневной витрины. |
| `mart_event_daily()` | Строит витрину день × тип события × органика. |
| `mart_user_segments()` | Строит пользовательские агрегаты и сегменты. |
| `mart_content_health()` | Считает популярность, качество прослушивания и tier трека. |
| `mart_user_general()` | Считает реакции и Algo/Organic listening-метрики на пользователя и период. |
| `write_mart()` | Выполняет streaming-запись и атомарно заменяет parquet с Zstandard-сжатием. |
| `build_all()` | Координирует полный ETL и запись пяти Parquet-витрин. |

---

## `validate_marts.py`

### Назначение

Скрипт выполняет независимые проверки файлов и бизнес-инвариантов. При любой
ошибке он завершает работу с ненулевым кодом и понятным исключением. При
успехе печатает основные объёмы, доли сегментов и размеры файлов.

Запуск:

```powershell
python scripts/validate_marts.py `
  --input data/processed/multi_event_clean.parquet `
  --marts-dir data/marts
```

| Аргумент | Обязательный | Значение |
|---|---|---|
| `--input PATH` | Нет | Clean parquet; по умолчанию `data/processed/multi_event_clean.parquet`. |
| `--marts-dir PATH` | Нет | Каталог витрин; по умолчанию `data/marts/`. |

### Проверки

- наличие всех parquet-файлов;
- точное совпадение обязательных столбцов всех пяти витрин;
- размер каждого файла не более 50 МиБ;
- уникальность ключей `time_period`, `(uid, time_period)`, `item_id`;
- положительный DAU;
- совпадение числа пользователей и треков с исходником;
- допустимость значений `segment` и `content_tier`;
- сумма долей пользовательских сегментов 100%;
- точное соответствие размеров Head, Torso и Tail заданным процентилям;
- независимое совпадение глобальных Algo/Organic listening, completion и skip
  с clean-слоем, включая пороги и bot/sequence-фильтры;
- равенство котловых content-метрик сумме Algo + Organic;

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
  `build_marts_v2.ipynb`.
- Для просмотра плана Polars перед выполнением можно вызвать `plan.explain()`.
- Если сборка завершилась, но проверка упала, не используйте
  `--skip-validation`-подобные обходы как постоянное решение: сначала
  устраните нарушенный инвариант (или обновите сам инвариант под новую
  схему — см. раздел `validate_marts.py` выше).
- После изменения правил витрин запускайте обе команды полного цикла и
  проверяйте `git diff --check` перед коммитом.
