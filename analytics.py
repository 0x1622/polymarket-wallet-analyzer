from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Sequence

from models import (
    AnalysisReport,
    BehaviorMetrics,
    EventMetadata,
    HeatmapBucket,
    LotMatch,
    MarketPnL,
    NormalizedTrade,
    OfficialSnapshots,
    OpenLot,
    PositionSnapshot,
    RankedStat,
    ResolvedTrader,
    TradeSide,
    WinLossRate,
)
from utils import (
    ProgressCallback,
    WEEKDAY_NAMES,
    dedupe_preserve_order,
    humanize_duration,
    mean_or_none,
    median_or_none,
    normalize_text,
    safe_div,
    weighted_average,
)


TOPIC_PRIORITY = [
    "elections",
    "crypto",
    "sports",
    "macro",
    "tech",
    "geopolitics",
    "business",
    "science",
    "culture",
    "politics",
    "other",
]

# Official category/tag labels are preferred. If Gamma metadata is sparse, these
# keyword rules provide a lightweight fallback classifier based on titles, slugs,
# descriptions, tags, and market types.
TOPIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "elections": (
        "election",
        "vote",
        "voting",
        "president",
        "senate",
        "house",
        "governor",
        "mayor",
        "ballot",
        "primary",
    ),
    "politics": (
        "politics",
        "trump",
        "biden",
        "democrat",
        "republican",
        "white house",
        "cabinet",
        "parliament",
        "congress",
        "government",
    ),
    "crypto": (
        "crypto",
        "bitcoin",
        "btc",
        "ethereum",
        "eth",
        "solana",
        "doge",
        "token",
        "defi",
        "stablecoin",
        "binance",
        "coinbase",
    ),
    "sports": (
        "nba",
        "nfl",
        "mlb",
        "nhl",
        "soccer",
        "football",
        "baseball",
        "basketball",
        "ufc",
        "mma",
        "tennis",
        "golf",
        "world cup",
        "championship",
    ),
    "macro": (
        "fed",
        "rates",
        "fomc",
        "inflation",
        "cpi",
        "interest rate",
        "economy",
        "economic",
        "gdp",
        "recession",
        "unemployment",
        "treasury",
        "oil",
        "gold",
    ),
    "tech": (
        "ai",
        "technology",
        "tech",
        "openai",
        "chatgpt",
        "apple",
        "google",
        "meta",
        "microsoft",
        "amazon",
        "nvidia",
        "tesla",
        "robot",
    ),
    "geopolitics": (
        "geopolitics",
        "war",
        "ceasefire",
        "ukraine",
        "russia",
        "china",
        "taiwan",
        "israel",
        "gaza",
        "nato",
        "military",
    ),
    "business": (
        "business",
        "finance",
        "financial",
        "ceo",
        "company",
        "corporate",
        "earnings",
        "revenue",
        "ipo",
        "stock",
        "shares",
        "acquisition",
        "merger",
        "bankruptcy",
    ),
    "science": (
        "science",
        "space",
        "nasa",
        "spacex",
        "climate",
        "weather",
        "hurricane",
        "earthquake",
        "scientist",
    ),
    "culture": (
        "culture",
        "food",
        "restaurant",
        "movie",
        "film",
        "oscar",
        "grammy",
        "music",
        "celebrity",
        "tv",
        "show",
        "box office",
    ),
}


@dataclass
class _Aggregate:
    trade_count: int = 0
    volume: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_pnl: float = 0.0


def normalize_trades(
    activities: Sequence,
    metadata_by_event_slug: dict[str, EventMetadata],
    progress_callback: ProgressCallback | None = None,
) -> list[NormalizedTrade]:
    """Normalize public activity records into the internal trade schema."""

    output: list[NormalizedTrade] = []
    topic_context_cache: dict[tuple[str, str], dict[str, object]] = {}
    total_activities = len(activities)
    if progress_callback is not None:
        progress_callback(0, total_activities, "Normalizing trades")

    for index, activity in enumerate(activities, start=1):
        if str(activity.type) != "TRADE":
            if progress_callback is not None and (
                index == total_activities or index % 1000 == 0
            ):
                progress_callback(
                    index,
                    total_activities,
                    f"Normalizing trades ({len(output):,} trades)",
                )
            continue
        if activity.side is None or activity.price is None or not activity.asset:
            if progress_callback is not None and (
                index == total_activities or index % 1000 == 0
            ):
                progress_callback(
                    index,
                    total_activities,
                    f"Normalizing trades ({len(output):,} trades)",
                )
            continue

        event_meta = metadata_by_event_slug.get(activity.event_slug or "")
        cache_key = (activity.event_slug or "", activity.condition_id)
        topic_context = topic_context_cache.get(cache_key)
        if topic_context is None:
            topic_context = infer_topic_context(activity, event_meta)
            topic_context_cache[cache_key] = topic_context
        event_title = event_meta.title if event_meta else None
        notional = (
            activity.usdc_size
            if activity.usdc_size is not None
            else activity.size * activity.price
        )

        output.append(
            NormalizedTrade(
                wallet=activity.proxy_wallet.casefold(),
                timestamp=datetime.fromtimestamp(activity.timestamp, tz=timezone.utc),
                timestamp_unix=activity.timestamp,
                side=activity.side,
                condition_id=activity.condition_id,
                asset_id=activity.asset,
                outcome=activity.outcome or "Unknown",
                outcome_index=activity.outcome_index,
                market_title=activity.title or "Unknown Market",
                market_slug=activity.slug,
                event_title=event_title,
                event_slug=activity.event_slug,
                event_id=activity.event_id,
                price=activity.price,
                quantity=activity.size,
                notional=notional,
                transaction_hash=activity.transaction_hash,
                profile_name=activity.name,
                pseudonym=activity.pseudonym,
                canonical_topic=topic_context["canonical_topic"],
                topic_source=topic_context["topic_source"],
                official_topics=topic_context["official_topics"],
                fallback_topics=topic_context["fallback_topics"],
                theme_labels=topic_context["theme_labels"],
                market_type=topic_context["market_type"],
            )
        )

        if progress_callback is not None and (
            index == total_activities or index % 1000 == 0
        ):
            progress_callback(
                index,
                total_activities,
                f"Normalizing trades ({len(output):,} trades)",
            )

    output.sort(
        key=lambda trade: (
            trade.timestamp,
            trade.transaction_hash or "",
            trade.asset_id,
        )
    )
    return output


def analyze_trader(
    *,
    trader: ResolvedTrader,
    trades: Sequence[NormalizedTrade],
    positions: Sequence[PositionSnapshot],
    metadata_by_event_slug: dict[str, EventMetadata],
    total_value: float | None,
    analysis_start: datetime | None,
    analysis_end: datetime,
    top_n: int,
    closed_positions_realized_pnl: float | None,
) -> AnalysisReport:
    """Run FIFO PnL and behavioral analytics over normalized trades."""

    report_trades = [
        trade
        for trade in trades
        if _in_window(trade.timestamp, analysis_start, analysis_end)
    ]
    touched_markets = {trade.condition_id for trade in report_trades}

    fifo_matches, open_lots, unmatched_sell_quantity = _run_fifo(trades)
    mark_lookup = build_mark_price_lookup(positions, metadata_by_event_slug)
    market_pnls = _aggregate_market_pnls(
        report_trades=report_trades,
        touched_markets=touched_markets,
        matches=fifo_matches,
        open_lots=open_lots,
        mark_lookup=mark_lookup,
        analysis_start=analysis_start,
        analysis_end=analysis_end,
    )

    total_volume = sum(trade.notional for trade in report_trades)
    realized_pnl = sum(market.realized_pnl for market in market_pnls)
    unrealized_pnl = sum(market.unrealized_pnl for market in market_pnls)
    total_pnl = realized_pnl + unrealized_pnl

    category_stats = _aggregate_categories(report_trades, market_pnls)
    theme_stats = _aggregate_themes(report_trades)
    market_type_stats = _aggregate_market_types(report_trades)
    behavior = _build_behavior_metrics(
        report_trades, fifo_matches, market_pnls, analysis_start, analysis_end
    )
    heatmap = _build_heatmap(report_trades)
    win_loss = _compute_win_loss(market_pnls)

    official_snapshots = OfficialSnapshots(
        total_value=total_value,
        current_positions_value=sum(
            (position.current_value or 0.0) for position in positions
        ),
        current_positions_cash_pnl=sum(
            (position.cash_pnl or 0.0) for position in positions
        ),
        current_positions_realized_pnl=sum(
            (position.realized_pnl or 0.0) for position in positions
        ),
        closed_positions_realized_pnl=closed_positions_realized_pnl,
    )

    assumptions = [
        "FIFO cost basis is applied chronologically per traded outcome token (asset).",
        "Realized PnL is recognized when a SELL closes earlier BUY lots for the same asset.",
        "Unrealized PnL is marked from public current-position prices when available; otherwise the latest public event market price is used.",
        "If a SELL appears without enough prior BUY inventory, the unmatched quantity is assigned a synthetic cost basis equal to the sell price so profit is not overstated.",
        "When --from-date is used, earlier trades are still used to seed FIFO cost basis, but only markets touched inside the analyzed window are reported.",
    ]
    if unmatched_sell_quantity > 0:
        assumptions.append(
            f"Unmatched SELL quantity encountered: {unmatched_sell_quantity:.4f} shares; those portions were treated conservatively with zero realized gain."
        )

    summary = _generate_summary(
        total_volume=total_volume,
        behavior=behavior,
        top_categories=category_stats,
        top_markets=sorted(market_pnls, key=lambda item: item.volume, reverse=True),
        win_loss=win_loss,
    )

    sorted_by_volume = sorted(market_pnls, key=lambda item: item.volume, reverse=True)
    sorted_by_count = sorted(
        market_pnls, key=lambda item: item.trade_count, reverse=True
    )
    sorted_by_realized_desc = sorted(
        market_pnls, key=lambda item: item.realized_pnl, reverse=True
    )
    sorted_by_realized_asc = sorted(market_pnls, key=lambda item: item.realized_pnl)

    return AnalysisReport(
        trader=trader,
        analysis_start=analysis_start,
        analysis_end=analysis_end,
        trade_count_total_history=len(trades),
        trade_count_in_window=len(report_trades),
        total_volume=total_volume,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        total_pnl=total_pnl,
        distinct_markets=len(touched_markets),
        win_loss=win_loss,
        top_categories=category_stats[:top_n],
        top_themes=theme_stats[:top_n],
        top_market_types=market_type_stats[:top_n],
        top_markets_by_count=sorted_by_count[:top_n],
        top_markets_by_volume=sorted_by_volume[:top_n],
        market_pnls=sorted_by_volume,
        best_markets_by_realized_pnl=sorted_by_realized_desc[:top_n],
        worst_markets_by_realized_pnl=sorted_by_realized_asc[:top_n],
        most_profitable_category=max(
            category_stats,
            key=lambda item: item.total_pnl or float("-inf"),
            default=None,
        ),
        least_profitable_category=min(
            category_stats,
            key=lambda item: item.total_pnl or float("inf"),
            default=None,
        ),
        behavior=behavior,
        heatmap=heatmap,
        summary=summary,
        assumptions=assumptions,
        official_snapshots=official_snapshots,
        normalized_trades=report_trades,
    )


def infer_topic_context(
    activity: object, event_meta: EventMetadata | None
) -> dict[str, object]:
    """Infer official topics and a single canonical category for a trade."""

    event_market = event_meta.find_market(activity.condition_id) if event_meta else None
    official_labels: list[str] = []
    if event_meta:
        if event_meta.category:
            official_labels.append(event_meta.category)
        if event_meta.subcategory:
            official_labels.append(event_meta.subcategory)
        official_labels.extend(
            category.label or "" for category in event_meta.categories
        )
        official_labels.extend(tag.label or "" for tag in event_meta.tags)
    if event_market:
        if event_market.category:
            official_labels.append(event_market.category)
        if event_market.market_type:
            official_labels.append(event_market.market_type)
        if event_market.sports_market_type:
            official_labels.append(event_market.sports_market_type)

    official_labels = dedupe_preserve_order(label for label in official_labels if label)
    official_mapped = [
        topic for label in official_labels if (topic := _map_to_topic(label))
    ]
    if official_mapped:
        canonical_topic = _pick_topic(official_mapped)
        return {
            "canonical_topic": canonical_topic,
            "topic_source": "official",
            "official_topics": official_labels,
            "fallback_topics": [],
            "theme_labels": official_labels or [canonical_topic],
            "market_type": event_market.market_type if event_market else None,
        }

    fallback_text_parts = [
        getattr(activity, "title", None),
        getattr(activity, "slug", None),
        getattr(activity, "event_slug", None),
        event_meta.title if event_meta else None,
        event_meta.description if event_meta else None,
        event_market.question if event_market else None,
        event_market.description if event_market else None,
    ]
    fallback_text = " ".join(piece for piece in fallback_text_parts if piece)
    fallback_topics = _infer_topics_from_text(fallback_text)
    canonical_topic = _pick_topic(fallback_topics) if fallback_topics else "other"
    theme_labels = official_labels or fallback_topics or [canonical_topic]
    return {
        "canonical_topic": canonical_topic,
        "topic_source": "fallback" if fallback_topics else "unknown",
        "official_topics": official_labels,
        "fallback_topics": fallback_topics,
        "theme_labels": theme_labels,
        "market_type": event_market.market_type if event_market else None,
    }


def build_mark_price_lookup(
    positions: Sequence[PositionSnapshot],
    metadata_by_event_slug: dict[str, EventMetadata],
) -> dict[str, float]:
    """Create an asset->mark price lookup from snapshots and public event prices."""

    lookup: dict[str, float] = {}
    for position in positions:
        if position.cur_price is not None:
            lookup[position.asset] = position.cur_price
    for event in metadata_by_event_slug.values():
        for market in event.markets:
            if len(market.clob_token_ids) != len(market.outcome_prices):
                continue
            for asset_id, price in zip(market.clob_token_ids, market.outcome_prices):
                lookup.setdefault(asset_id, price)
    return lookup


def _run_fifo(
    trades: Sequence[NormalizedTrade],
) -> tuple[list[LotMatch], list[OpenLot], float]:
    lots_by_asset: dict[str, deque[OpenLot]] = defaultdict(deque)
    matches: list[LotMatch] = []
    unmatched_sell_quantity = 0.0

    for trade in trades:
        if trade.side == TradeSide.BUY:
            lots_by_asset[trade.asset_id].append(
                OpenLot(
                    asset_id=trade.asset_id,
                    condition_id=trade.condition_id,
                    event_slug=trade.event_slug,
                    market_title=trade.market_title,
                    outcome=trade.outcome,
                    canonical_topic=trade.canonical_topic,
                    topic_source=trade.topic_source,
                    official_topics=trade.official_topics,
                    market_type=trade.market_type,
                    open_timestamp=trade.timestamp,
                    remaining_quantity=trade.quantity,
                    entry_price=trade.price,
                )
            )
            continue

        remaining = trade.quantity
        queue = lots_by_asset[trade.asset_id]
        while remaining > 1e-12 and queue:
            lot = queue[0]
            matched_quantity = min(remaining, lot.remaining_quantity)
            hold_seconds = (trade.timestamp - lot.open_timestamp).total_seconds()
            matches.append(
                LotMatch(
                    asset_id=trade.asset_id,
                    condition_id=trade.condition_id,
                    event_slug=trade.event_slug,
                    market_title=trade.market_title,
                    outcome=trade.outcome,
                    canonical_topic=trade.canonical_topic,
                    close_timestamp=trade.timestamp,
                    open_timestamp=lot.open_timestamp,
                    quantity=matched_quantity,
                    entry_price=lot.entry_price,
                    exit_price=trade.price,
                    realized_pnl=matched_quantity * (trade.price - lot.entry_price),
                    hold_seconds=hold_seconds,
                )
            )
            lot.remaining_quantity -= matched_quantity
            remaining -= matched_quantity
            if lot.remaining_quantity <= 1e-12:
                queue.popleft()

        if remaining > 1e-12:
            unmatched_sell_quantity += remaining
            matches.append(
                LotMatch(
                    asset_id=trade.asset_id,
                    condition_id=trade.condition_id,
                    event_slug=trade.event_slug,
                    market_title=trade.market_title,
                    outcome=trade.outcome,
                    canonical_topic=trade.canonical_topic,
                    close_timestamp=trade.timestamp,
                    quantity=remaining,
                    entry_price=trade.price,
                    exit_price=trade.price,
                    realized_pnl=0.0,
                    synthetic_cost_basis=True,
                )
            )

    open_lots = [
        lot
        for queue in lots_by_asset.values()
        for lot in queue
        if lot.remaining_quantity > 1e-12
    ]
    return matches, open_lots, unmatched_sell_quantity


def _aggregate_market_pnls(
    *,
    report_trades: Sequence[NormalizedTrade],
    touched_markets: set[str],
    matches: Sequence[LotMatch],
    open_lots: Sequence[OpenLot],
    mark_lookup: dict[str, float],
    analysis_start: datetime | None,
    analysis_end: datetime,
) -> list[MarketPnL]:
    by_market: dict[str, MarketPnL] = {}

    def ensure_market_from_trade(trade: NormalizedTrade) -> MarketPnL:
        market = by_market.get(trade.condition_id)
        if market is None:
            market = MarketPnL(
                condition_id=trade.condition_id,
                market_title=trade.market_title,
                event_slug=trade.event_slug,
                event_title=trade.event_title,
                canonical_topic=trade.canonical_topic,
                official_topics=trade.official_topics,
                market_type=trade.market_type,
            )
            by_market[trade.condition_id] = market
        return market

    for trade in report_trades:
        market = ensure_market_from_trade(trade)
        market.trade_count += 1
        market.volume += trade.notional
        if trade.side == TradeSide.BUY:
            market.buy_count += 1
        else:
            market.sell_count += 1
        if normalize_text(trade.outcome) == "yes":
            market.yes_trade_count += 1
        elif normalize_text(trade.outcome) == "no":
            market.no_trade_count += 1

    for match in matches:
        if match.condition_id not in touched_markets:
            continue
        if not _in_window(match.close_timestamp, analysis_start, analysis_end):
            continue
        market = by_market.setdefault(
            match.condition_id,
            MarketPnL(
                condition_id=match.condition_id,
                market_title=match.market_title,
                event_slug=match.event_slug,
                canonical_topic=match.canonical_topic,
            ),
        )
        market.realized_pnl += match.realized_pnl

    for lot in open_lots:
        if lot.condition_id not in touched_markets:
            continue
        mark_price = mark_lookup.get(lot.asset_id, lot.entry_price)
        market = by_market.setdefault(
            lot.condition_id,
            MarketPnL(
                condition_id=lot.condition_id,
                market_title=lot.market_title,
                event_slug=lot.event_slug,
                canonical_topic=lot.canonical_topic,
                official_topics=lot.official_topics,
                market_type=lot.market_type,
            ),
        )
        market.unrealized_pnl += lot.remaining_quantity * (mark_price - lot.entry_price)
        market.open_quantity += lot.remaining_quantity

    total_volume = sum(market.volume for market in by_market.values())
    for market in by_market.values():
        market.total_pnl = market.realized_pnl + market.unrealized_pnl
        market.share_of_volume = safe_div(market.volume, total_volume)

    return list(by_market.values())


def _aggregate_categories(
    trades: Sequence[NormalizedTrade], market_pnls: Sequence[MarketPnL]
) -> list[RankedStat]:
    aggregates: dict[str, _Aggregate] = defaultdict(_Aggregate)
    for trade in trades:
        aggregate = aggregates[trade.canonical_topic]
        aggregate.trade_count += 1
        aggregate.volume += trade.notional

    for market in market_pnls:
        aggregate = aggregates[market.canonical_topic]
        aggregate.realized_pnl += market.realized_pnl
        aggregate.unrealized_pnl += market.unrealized_pnl
        aggregate.total_pnl += market.total_pnl

    total_volume = sum(item.volume for item in aggregates.values())
    ranked = [
        RankedStat(
            name=name,
            trade_count=value.trade_count,
            volume=value.volume,
            realized_pnl=value.realized_pnl,
            unrealized_pnl=value.unrealized_pnl,
            total_pnl=value.total_pnl,
            share_of_volume=safe_div(value.volume, total_volume),
        )
        for name, value in aggregates.items()
    ]
    return sorted(ranked, key=lambda item: item.volume, reverse=True)


def _aggregate_themes(trades: Sequence[NormalizedTrade]) -> list[RankedStat]:
    aggregates: dict[str, _Aggregate] = defaultdict(_Aggregate)
    for trade in trades:
        labels = trade.theme_labels or [trade.canonical_topic]
        for label in dedupe_preserve_order(labels):
            aggregate = aggregates[label]
            aggregate.trade_count += 1
            aggregate.volume += trade.notional
    total_volume = sum(trade.notional for trade in trades)
    ranked = [
        RankedStat(
            name=name,
            trade_count=value.trade_count,
            volume=value.volume,
            share_of_volume=safe_div(value.volume, total_volume),
        )
        for name, value in aggregates.items()
    ]
    return sorted(ranked, key=lambda item: item.volume, reverse=True)


def _aggregate_market_types(trades: Sequence[NormalizedTrade]) -> list[RankedStat]:
    aggregates: dict[str, _Aggregate] = defaultdict(_Aggregate)
    for trade in trades:
        if not trade.market_type:
            continue
        aggregate = aggregates[trade.market_type]
        aggregate.trade_count += 1
        aggregate.volume += trade.notional
    total_volume = sum(trade.notional for trade in trades)
    ranked = [
        RankedStat(
            name=name,
            trade_count=value.trade_count,
            volume=value.volume,
            share_of_volume=safe_div(value.volume, total_volume),
        )
        for name, value in aggregates.items()
    ]
    return sorted(ranked, key=lambda item: item.volume, reverse=True)


def _build_behavior_metrics(
    trades: Sequence[NormalizedTrade],
    matches: Sequence[LotMatch],
    market_pnls: Sequence[MarketPnL],
    analysis_start: datetime | None,
    analysis_end: datetime,
) -> BehaviorMetrics:
    buy_trades = [trade for trade in trades if trade.side == TradeSide.BUY]
    sell_trades = [trade for trade in trades if trade.side == TradeSide.SELL]
    yes_trades = [trade for trade in trades if normalize_text(trade.outcome) == "yes"]
    no_trades = [trade for trade in trades if normalize_text(trade.outcome) == "no"]
    closed_matches = [
        match
        for match in matches
        if _in_window(match.close_timestamp, analysis_start, analysis_end)
        and not match.synthetic_cost_basis
    ]

    hour_counts: dict[int, int] = defaultdict(int)
    hour_volume: dict[int, float] = defaultdict(float)
    day_counts: dict[str, int] = defaultdict(int)
    day_volume: dict[str, float] = defaultdict(float)
    market_volume: dict[str, float] = defaultdict(float)
    event_volume: dict[str, float] = defaultdict(float)
    category_volume: dict[str, float] = defaultdict(float)

    for trade in trades:
        hour_counts[trade.timestamp.hour] += 1
        hour_volume[trade.timestamp.hour] += trade.notional
        day_name = WEEKDAY_NAMES[trade.timestamp.weekday()]
        day_counts[day_name] += 1
        day_volume[day_name] += trade.notional
        market_volume[trade.market_title] += trade.notional
        event_volume[trade.event_title or trade.event_slug or trade.market_title] += (
            trade.notional
        )
        category_volume[trade.canonical_topic] += trade.notional

    total_volume = sum(trade.notional for trade in trades)
    favorite_topics = [
        item.name
        for item in sorted(
            _aggregate_categories(trades, market_pnls),
            key=lambda item: item.volume,
            reverse=True,
        )[:3]
    ]
    favorite_market_types = [item.name for item in _aggregate_market_types(trades)[:3]]
    peak_hours = [
        hour
        for hour, _ in sorted(
            hour_volume.items(), key=lambda item: item[1], reverse=True
        )[:3]
    ]
    peak_days = [
        day
        for day, _ in sorted(
            day_volume.items(), key=lambda item: item[1], reverse=True
        )[:3]
    ]

    return BehaviorMetrics(
        buy_count=len(buy_trades),
        sell_count=len(sell_trades),
        buy_volume=sum(trade.notional for trade in buy_trades),
        sell_volume=sum(trade.notional for trade in sell_trades),
        buy_sell_ratio=(len(buy_trades) / len(sell_trades)) if sell_trades else None,
        yes_count=len(yes_trades),
        no_count=len(no_trades),
        yes_volume=sum(trade.notional for trade in yes_trades),
        no_volume=sum(trade.notional for trade in no_trades),
        yes_preference_ratio=safe_div(len(yes_trades), len(yes_trades) + len(no_trades))
        if (yes_trades or no_trades)
        else None,
        average_entry_price=weighted_average(
            (trade.price, trade.quantity) for trade in buy_trades
        ),
        average_exit_price=weighted_average(
            (trade.price, trade.quantity) for trade in sell_trades
        ),
        average_position_size=mean_or_none([trade.notional for trade in trades]),
        median_position_size=median_or_none([trade.notional for trade in trades]),
        average_hold_seconds=weighted_average(
            (match.hold_seconds or 0.0, match.quantity) for match in closed_matches
        ),
        favorite_topics=favorite_topics,
        favorite_market_types=favorite_market_types,
        peak_hours_utc=peak_hours,
        peak_days_utc=peak_days,
        top_market_concentration=safe_div(
            max(market_volume.values(), default=0.0), total_volume
        ),
        top_event_concentration=safe_div(
            max(event_volume.values(), default=0.0), total_volume
        ),
        top_category_concentration=safe_div(
            max(category_volume.values(), default=0.0), total_volume
        ),
    )


def _build_heatmap(trades: Sequence[NormalizedTrade]) -> list[HeatmapBucket]:
    buckets: dict[tuple[int, int], _Aggregate] = defaultdict(_Aggregate)
    for trade in trades:
        key = (trade.timestamp.weekday(), trade.timestamp.hour)
        bucket = buckets[key]
        bucket.trade_count += 1
        bucket.volume += trade.notional

    output = [
        HeatmapBucket(
            day_of_week=WEEKDAY_NAMES[day_index],
            day_index=day_index,
            hour_utc=hour_utc,
            trade_count=aggregate.trade_count,
            volume=aggregate.volume,
        )
        for (day_index, hour_utc), aggregate in sorted(buckets.items())
    ]
    return output


def _compute_win_loss(market_pnls: Sequence[MarketPnL]) -> WinLossRate:
    wins = sum(1 for market in market_pnls if market.total_pnl > 1e-12)
    losses = sum(1 for market in market_pnls if market.total_pnl < -1e-12)
    flat = len(market_pnls) - wins - losses
    denominator = wins + losses
    return WinLossRate(
        wins=wins,
        losses=losses,
        flat=flat,
        win_rate=safe_div(wins, denominator),
        loss_rate=safe_div(losses, denominator),
    )


def _generate_summary(
    *,
    total_volume: float,
    behavior: BehaviorMetrics,
    top_categories: Sequence[RankedStat],
    top_markets: Sequence[MarketPnL],
    win_loss: WinLossRate,
) -> str:
    sentences: list[str] = []
    if top_categories:
        category_names = " and ".join(item.name for item in top_categories[:2])
        first_sentence = f"This account mostly trades {category_names} markets"
        if top_markets and total_volume > 0:
            top_market = top_markets[0]
            first_sentence += (
                f", with {top_market.share_of_volume * 100:.1f}% of analyzed volume in "
                f"'{top_market.market_title}'"
            )
        sentences.append(first_sentence + ".")

    second_sentence_parts: list[str] = []
    if behavior.yes_preference_ratio is not None:
        yes_pct = behavior.yes_preference_ratio * 100
        bias = (
            "YES" if yes_pct > 55 else "NO" if yes_pct < 45 else "a balanced YES/NO mix"
        )
        if bias == "a balanced YES/NO mix":
            second_sentence_parts.append("It uses a fairly balanced YES/NO mix")
        else:
            second_sentence_parts.append(
                f"It leans {bias} ({yes_pct:.1f}% YES by trade count)"
            )
    if behavior.buy_sell_ratio is not None:
        direction = (
            "buy-heavy"
            if behavior.buy_sell_ratio > 1
            else "sell-heavy"
            if behavior.buy_sell_ratio < 1
            else "balanced"
        )
        second_sentence_parts.append(f"order flow is {direction}")
    if second_sentence_parts:
        sentences.append(
            ", and ".join(
                part if index == 0 else part[0].lower() + part[1:]
                for index, part in enumerate(second_sentence_parts)
            )
            + "."
        )

    third_sentence_parts: list[str] = []
    if behavior.average_hold_seconds is not None:
        third_sentence_parts.append(
            f"Closed positions are held about {humanize_duration(behavior.average_hold_seconds)} on average"
        )
    if behavior.peak_hours_utc:
        hours = ", ".join(f"{hour:02d}:00" for hour in behavior.peak_hours_utc[:2])
        third_sentence_parts.append(f"activity is strongest around {hours} UTC")
    if win_loss.wins or win_loss.losses:
        third_sentence_parts.append(
            f"the analyzed market win rate is {win_loss.win_rate * 100:.1f}%"
        )
    if third_sentence_parts:
        sentences.append(
            ", and ".join(
                part if index == 0 else part[0].lower() + part[1:]
                for index, part in enumerate(third_sentence_parts)
            )
            + "."
        )

    summary = " ".join(sentences).strip()
    if not summary:
        return "This account has too little trade history in the selected window to infer a reliable behavior summary."
    return summary


def _map_to_topic(label: str) -> str | None:
    normalized = normalize_text(label)
    if not normalized:
        return None
    for topic in TOPIC_PRIORITY:
        if topic == "other":
            continue
        for keyword in TOPIC_KEYWORDS[topic]:
            if _keyword_match(normalized, keyword):
                return topic
    return None


def _infer_topics_from_text(text: str) -> list[str]:
    normalized = normalize_text(text)
    topics: list[str] = []
    for topic in TOPIC_PRIORITY:
        if topic == "other":
            continue
        if any(
            _keyword_match(normalized, keyword) for keyword in TOPIC_KEYWORDS[topic]
        ):
            topics.append(topic)
    return topics


def _keyword_match(normalized_text: str, keyword: str) -> bool:
    padded_text = f" {normalized_text} "
    padded_keyword = f" {normalize_text(keyword)} "
    return padded_keyword in padded_text


def _pick_topic(topics: Iterable[str]) -> str:
    topic_set = {topic for topic in topics}
    for topic in TOPIC_PRIORITY:
        if topic in topic_set:
            return topic
    return "other"


def _in_window(timestamp: datetime, start: datetime | None, end: datetime) -> bool:
    if start is not None and timestamp < start:
        return False
    return timestamp <= end
