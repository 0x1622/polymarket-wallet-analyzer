from __future__ import annotations

import csv
import logging
import re
from datetime import date, datetime, time, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Sequence

from models import MarketPnL, NormalizedTrade


UTC = timezone.utc
WEEKDAY_NAMES = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


class PolymarketError(Exception):
    """Base error for CLI-friendly failures."""


class PolymarketAPIError(PolymarketError):
    """Raised when a Polymarket HTTP request fails."""


class ResolutionError(PolymarketError):
    """Raised when a trader name or wallet cannot be resolved safely."""


class AmbiguousResolutionError(ResolutionError):
    """Raised when multiple profile matches are plausible."""

    def __init__(self, message: str, candidates: Sequence[dict[str, Any]]) -> None:
        super().__init__(message)
        self.candidates = list(candidates)


class NoTradesFoundError(PolymarketError):
    """Raised when the target account has no relevant trades to analyze."""


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.INFO if verbose else logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.INFO if verbose else logging.WARNING)


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def parse_iso_date(value: str | None) -> date | None:
    if value is None:
        return None
    return date.fromisoformat(value)


def start_of_day_utc(value: date | None) -> datetime | None:
    if value is None:
        return None
    return datetime.combine(value, time.min, tzinfo=UTC)


def end_of_day_utc(value: date | None) -> datetime | None:
    if value is None:
        return None
    return datetime.combine(value, time.max, tzinfo=UTC)


def safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def weighted_average(pairs: Iterable[tuple[float, float]]) -> float | None:
    total_weight = 0.0
    total_value = 0.0
    for value, weight in pairs:
        if weight <= 0:
            continue
        total_weight += weight
        total_value += value * weight
    if total_weight <= 0:
        return None
    return total_value / total_weight


def mean_or_none(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def median_or_none(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return float(median(values))


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    cleaned = value.casefold()
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        cleaned = value.strip()
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(cleaned)
    return output


def is_wallet_address(value: str | None) -> bool:
    if value is None:
        return False
    return bool(re.fullmatch(r"0x[a-fA-F0-9]{40}", value))


def humanize_duration(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    total_seconds = int(round(seconds))
    if total_seconds < 60:
        return f"{total_seconds}s"
    minutes, seconds_part = divmod(total_seconds, 60)
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes_part = divmod(minutes, 60)
    if hours < 48:
        return f"{hours}h {minutes_part}m"
    days, hours_part = divmod(hours, 24)
    return f"{days}d {hours_part}h"


def format_money(value: float) -> str:
    return f"${value:,.2f}"


def format_percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def make_csv_paths(csv_out: str) -> tuple[Path, Path]:
    base_path = Path(csv_out)
    if base_path.suffix.lower() == ".csv":
        stem_path = base_path.with_suffix("")
    else:
        stem_path = base_path
    trades_path = stem_path.with_name(f"{stem_path.name}_trades.csv")
    markets_path = stem_path.with_name(f"{stem_path.name}_markets.csv")
    return trades_path, markets_path


def _serialize_csv_value(value: Any) -> str | float | int:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return " | ".join(str(item) for item in value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def export_csvs(
    csv_out: str, trades: Sequence[NormalizedTrade], market_pnls: Sequence[MarketPnL]
) -> tuple[Path, Path]:
    trades_path, markets_path = make_csv_paths(csv_out)
    trades_path.parent.mkdir(parents=True, exist_ok=True)
    markets_path.parent.mkdir(parents=True, exist_ok=True)

    trade_rows = [trade.model_dump() for trade in trades]
    market_rows = [market.model_dump() for market in market_pnls]

    _write_csv(trades_path, trade_rows)
    _write_csv(markets_path, market_rows)
    return trades_path, markets_path


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {key: _serialize_csv_value(value) for key, value in row.items()}
            )
