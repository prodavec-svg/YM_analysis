"""Build presentation-ready charts from the three Yambda data marts."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from matplotlib.ticker import FuncFormatter, PercentFormatter


PROJECT_DIR = Path(__file__).resolve().parents[1]
MARTS_DIR = PROJECT_DIR / "data" / "marts"
REPORT_DIR = PROJECT_DIR / "reports"
FIGURES_DIR = REPORT_DIR / "figures"

BACKGROUND = "#111315"
PANEL = "#191C1F"
FOREGROUND = "#F4F4F2"
MUTED = "#A6ABB2"
GRID = "#34383D"
YELLOW = "#FFCC00"
CYAN = "#55C2D9"
ORANGE = "#F28E2B"
GREEN = "#59A14F"
PURPLE = "#B07AA1"

SEGMENT_ORDER = ["Cold", "Explorer", "Mixed", "Passive"]
SEGMENT_COLORS = {
    "Cold": CYAN,
    "Explorer": GREEN,
    "Mixed": YELLOW,
    "Passive": PURPLE,
}
TIER_ORDER = ["Head", "Torso", "Tail"]
TIER_COLORS = {"Head": ORANGE, "Torso": YELLOW, "Tail": CYAN}


def configure_style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": BACKGROUND,
            "axes.facecolor": PANEL,
            "axes.edgecolor": GRID,
            "axes.labelcolor": MUTED,
            "axes.titlecolor": FOREGROUND,
            "axes.titlesize": 14,
            "axes.titleweight": "bold",
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "text.color": FOREGROUND,
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "grid.color": GRID,
            "grid.alpha": 0.65,
            "legend.facecolor": PANEL,
            "legend.edgecolor": GRID,
            "legend.labelcolor": FOREGROUND,
        }
    )


def read_marts() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    paths = {
        "daily": MARTS_DIR / "mart_daily_metrics.parquet",
        "users": MARTS_DIR / "mart_user_segments.parquet",
        "content": MARTS_DIR / "mart_content_health.parquet",
    }
    missing = [str(path) for path in paths.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "Missing marts. Run scripts/build_marts.py first:\n  - "
            + "\n  - ".join(missing)
        )
    return (
        pl.read_parquet(paths["daily"]).sort("time_period"),
        pl.read_parquet(paths["users"]),
        pl.read_parquet(paths["content"]).sort(
            ["total_likes", "item_id"], descending=[True, False]
        ),
    )


def finish_figure(figure: plt.Figure, filename: str) -> None:
    figure.savefig(
        FIGURES_DIR / filename,
        dpi=180,
        bbox_inches="tight",
        facecolor=figure.get_facecolor(),
    )
    plt.close(figure)


def plot_daily_activity(daily: pl.DataFrame) -> None:
    daily = daily.with_columns(
        pl.col("dau").rolling_mean(window_size=30, min_samples=1).alias("dau_30d"),
        pl.col("total_interactions")
        .rolling_mean(window_size=30, min_samples=1)
        .alias("interactions_30d"),
        pl.col("organic_ratio")
        .rolling_mean(window_size=30, min_samples=1)
        .alias("organic_30d"),
    )
    x = daily["time_period"].to_numpy()
    series = [
        ("dau", "dau_30d", "DAU", "пользователей", CYAN),
        (
            "total_interactions",
            "interactions_30d",
            "Активность",
            "взаимодействий",
            YELLOW,
        ),
        (
            "organic_ratio",
            "organic_30d",
            "Органическая доля",
            "доля",
            GREEN,
        ),
    ]
    figure, axes = plt.subplots(3, 1, figsize=(13, 9), sharex=True)
    figure.suptitle("Динамика продукта по условным дням", fontsize=19, weight="bold")
    figure.subplots_adjust(hspace=0.25)

    for axis, (raw, smooth, title, unit, color) in zip(axes, series, strict=True):
        raw_values = daily[raw].to_numpy()
        smooth_values = daily[smooth].to_numpy()
        axis.plot(x, raw_values, color=color, alpha=0.20, linewidth=0.8, label="День")
        axis.plot(x, smooth_values, color=color, linewidth=2.0, label="30-дневное среднее")
        axis.set_title(title, loc="left")
        axis.set_ylabel(unit)
        axis.grid(axis="y")
        axis.spines[["top", "right"]].set_visible(False)
        axis.legend(loc="upper left", frameon=False, ncols=2)
    axes[-1].set_xlabel("Условный день (timestamp // 17 280)")
    axes[-1].yaxis.set_major_formatter(PercentFormatter(1.0))
    finish_figure(figure, "daily-activity.png")


def segment_summary(users: pl.DataFrame) -> list[dict]:
    rows = (
        users.group_by("segment")
        .agg(
            pl.len().alias("users"),
            pl.col("total_likes").mean().alias("avg_likes"),
            (
                pl.col("organic_likes").sum() / pl.col("total_likes").sum()
            ).alias("organic_share"),
        )
        .to_dicts()
    )
    lookup = {row["segment"]: row for row in rows}
    return [lookup[name] for name in SEGMENT_ORDER]


def plot_user_segments(users: pl.DataFrame) -> list[dict]:
    rows = segment_summary(users)
    total_users = users.height
    shares = np.array([row["users"] / total_users for row in rows])
    avg_likes = np.array([row["avg_likes"] for row in rows])
    labels = SEGMENT_ORDER
    colors = [SEGMENT_COLORS[label] for label in labels]

    figure, (left, right) = plt.subplots(1, 2, figsize=(14, 5.7))
    figure.suptitle("Поведенческие сегменты аудитории", fontsize=19, weight="bold")
    figure.subplots_adjust(top=0.78, wspace=0.34, left=0.08, right=0.98)

    bars = left.barh(labels, shares, color=colors, height=0.62)
    left.invert_yaxis()
    left.set_title("Доля пользователей", loc="left")
    left.set_xlabel("доля от всей аудитории")
    left.set_xlim(0, 0.48)
    left.xaxis.set_major_formatter(PercentFormatter(1.0))
    left.grid(axis="x")
    left.spines[["top", "right", "left"]].set_visible(False)
    left.bar_label(
        bars,
        labels=[f"{share:.1%} · {row['users']:,}" for share, row in zip(shares, rows)],
        padding=5,
        color=FOREGROUND,
    )

    bars = right.bar(labels, avg_likes, color=colors, width=0.62)
    right.set_title("Среднее число лайков на пользователя", loc="left")
    right.set_ylabel("лайков")
    right.set_ylim(0, avg_likes.max() * 1.30)
    right.grid(axis="y")
    right.spines[["top", "right"]].set_visible(False)
    right.bar_label(
        bars,
        labels=[
            f"{row['avg_likes']:.1f}\norg {row['organic_share']:.0%}" for row in rows
        ],
        padding=4,
        color=FOREGROUND,
    )
    finish_figure(figure, "user-segments.png")
    return rows


def content_summary(content: pl.DataFrame) -> list[dict]:
    rows = (
        content.group_by("content_tier")
        .agg(
            pl.len().alias("items"),
            pl.col("total_likes").sum().alias("likes"),
            pl.col("total_likes").mean().alias("avg_likes"),
            (
                (pl.col("organic_ratio") * pl.col("total_likes")).sum()
                / pl.col("total_likes").sum()
            ).alias("organic_share"),
        )
        .to_dicts()
    )
    lookup = {row["content_tier"]: row for row in rows}
    return [lookup[name] for name in TIER_ORDER]


def calculate_gini(values: np.ndarray) -> float:
    ordered = np.sort(values.astype(np.float64))
    n = ordered.size
    return float(
        (2 * np.sum(np.arange(1, n + 1) * ordered) / (n * ordered.sum()))
        - (n + 1) / n
    )


def plot_content_health(content: pl.DataFrame) -> tuple[list[dict], float]:
    rows = content_summary(content)
    total_items = content.height
    total_likes = int(content["total_likes"].sum())
    item_shares = np.array([row["items"] / total_items for row in rows])
    like_shares = np.array([row["likes"] / total_likes for row in rows])
    likes = content["total_likes"].to_numpy()
    ranks = np.arange(1, total_items + 1)
    tiers = content["content_tier"].to_numpy()
    gini = calculate_gini(likes)

    figure, (left, right) = plt.subplots(1, 2, figsize=(13, 5.3))
    figure.suptitle("Здоровье каталога и popularity bias", fontsize=19, weight="bold")

    for tier in TIER_ORDER:
        mask = tiers == tier
        left.plot(
            ranks[mask],
            likes[mask],
            color=TIER_COLORS[tier],
            linewidth=1.8,
            label=tier,
        )
    left.set_xscale("log")
    left.set_yscale("log")
    left.set_title(f"Ранговая кривая популярности · Gini {gini:.3f}", loc="left")
    left.set_xlabel("ранг трека (log)")
    left.set_ylabel("лайков (log)")
    left.grid(which="both", alpha=0.38)
    left.spines[["top", "right"]].set_visible(False)
    left.legend(frameon=False)

    positions = np.arange(len(TIER_ORDER))
    width = 0.34
    catalog_bars = right.bar(
        positions - width / 2,
        item_shares,
        width,
        color=MUTED,
        label="Доля каталога",
    )
    traffic_bars = right.bar(
        positions + width / 2,
        like_shares,
        width,
        color=[TIER_COLORS[tier] for tier in TIER_ORDER],
        label="Доля лайков",
    )
    right.set_xticks(positions, TIER_ORDER)
    right.set_title("Каталог против полученного внимания", loc="left")
    right.set_ylabel("доля")
    right.yaxis.set_major_formatter(PercentFormatter(1.0))
    right.grid(axis="y")
    right.spines[["top", "right"]].set_visible(False)
    right.legend(frameon=False)
    right.bar_label(
        catalog_bars,
        labels=[f"{value:.0%}" for value in item_shares],
        padding=3,
        color=FOREGROUND,
    )
    right.bar_label(
        traffic_bars,
        labels=[f"{value:.1%}" for value in like_shares],
        padding=3,
        color=FOREGROUND,
    )
    finish_figure(figure, "content-health.png")
    return rows, gini


def write_report(
    daily: pl.DataFrame,
    users: pl.DataFrame,
    content: pl.DataFrame,
    segments: list[dict],
    tiers: list[dict],
    gini: float,
) -> None:
    interactions = int(daily["total_interactions"].sum())
    organic_likes = int(users["organic_likes"].sum())
    avg_dau = float(daily["dau"].mean())
    peak_dau = int(daily["dau"].max())
    peak_day = int(daily.sort("dau", descending=True)[0, "time_period"])
    head = tiers[0]
    torso = tiers[1]
    head_like_share = head["likes"] / interactions
    top_20_share = (head["likes"] + torso["likes"]) / interactions

    segment_lines = "\n".join(
        f"| {row['segment']} | {row['users']:,} | {row['users'] / users.height:.2%} "
        f"| {row['avg_likes']:.1f} | {row['organic_share']:.1%} |"
        for row in segments
    )
    tier_lines = "\n".join(
        f"| {row['content_tier']} | {row['items']:,} | {row['items'] / content.height:.2%} "
        f"| {row['likes']:,} | {row['likes'] / interactions:.2%} |"
        for row in tiers
    )
    report = f"""# Yambda: обзор датасета

Сводка автоматически построена из витрин в `data/marts/`.

| Метрика | Значение |
|---|---:|
| Взаимодействия | {interactions:,} |
| Пользователи | {users.height:,} |
| Треки | {content.height:,} |
| Условные дни | {daily.height:,} |
| Средний DAU | {avg_dau:.1f} |
| Пиковый DAU | {peak_dau:,} (день {peak_day}) |
| Органическая доля | {organic_likes / interactions:.2%} |
| Gini популярности | {gini:.3f} |

## Динамика продукта

![DAU, активность и органическая доля по времени](figures/daily-activity.png)

## Сегменты аудитории

![Размер и активность пользовательских сегментов](figures/user-segments.png)

| Сегмент | Пользователи | Доля | Лайков в среднем | Organic |
|---|---:|---:|---:|---:|
{segment_lines}

## Состояние каталога

![Ранговая кривая и распределение внимания](figures/content-health.png)

| Tier | Треки | Доля каталога | Лайки | Доля лайков |
|---|---:|---:|---:|---:|
{tier_lines}

Топ-1% каталога собирает **{head_like_share:.1%}** всех лайков, а верхние 20% —
**{top_20_share:.1%}**. Это заметная концентрация внимания, хотя Gini {gini:.3f}
не указывает на экстремальную монополизацию каталога.
"""
    REPORT_DIR.joinpath("README.md").write_text(report, encoding="utf-8")


def main() -> None:
    configure_style()
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    daily, users, content = read_marts()
    plot_daily_activity(daily)
    segments = plot_user_segments(users)
    tiers, gini = plot_content_health(content)
    write_report(daily, users, content, segments, tiers, gini)
    print(f"Built report: {REPORT_DIR / 'README.md'}")


if __name__ == "__main__":
    main()
