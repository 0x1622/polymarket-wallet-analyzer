from __future__ import annotations

import json
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator


def _parse_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return [piece.strip() for piece in raw.split(",") if piece.strip()]
        if isinstance(parsed, list):
            return parsed
        return [parsed]
    return [value]


class TradeSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class ActivityKind(StrEnum):
    TRADE = "TRADE"
    SPLIT = "SPLIT"
    MERGE = "MERGE"
    REDEEM = "REDEEM"
    REWARD = "REWARD"
    CONVERSION = "CONVERSION"


class ApiCredentials(BaseModel):
    """Optional future-proof API credentials.

    Public analytics does not require these today, but the CLI accepts them so
    authenticated endpoints can be layered in later without changing the CLI.
    """

    api_key: str | None = None
    api_secret: str | None = None
    api_passphrase: str | None = None


class PublicProfileUser(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    creator: bool | None = None
    mod: bool | None = None
    community_mod: bool | None = Field(
        default=None, validation_alias=AliasChoices("communityMod")
    )


class PublicProfile(BaseModel):
    model_config = ConfigDict(extra="ignore")

    created_at: datetime | None = Field(
        default=None, validation_alias=AliasChoices("createdAt")
    )
    proxy_wallet: str | None = Field(
        default=None, validation_alias=AliasChoices("proxyWallet")
    )
    profile_image: str | None = Field(
        default=None, validation_alias=AliasChoices("profileImage")
    )
    display_username_public: bool | None = Field(
        default=None,
        validation_alias=AliasChoices("displayUsernamePublic"),
    )
    bio: str | None = None
    pseudonym: str | None = None
    name: str | None = None
    users: list[PublicProfileUser] = Field(default_factory=list)
    x_username: str | None = Field(
        default=None, validation_alias=AliasChoices("xUsername")
    )
    verified_badge: bool | None = Field(
        default=None, validation_alias=AliasChoices("verifiedBadge")
    )


class SearchPagination(BaseModel):
    model_config = ConfigDict(extra="ignore")

    has_more: bool = Field(default=False, validation_alias=AliasChoices("hasMore"))
    total_results: int = Field(default=0, validation_alias=AliasChoices("totalResults"))


class SearchProfile(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    name: str | None = None
    user: int | None = None
    created_at: datetime | None = Field(
        default=None, validation_alias=AliasChoices("createdAt")
    )
    updated_at: datetime | None = Field(
        default=None, validation_alias=AliasChoices("updatedAt")
    )
    wallet_activated: bool | None = Field(
        default=None, validation_alias=AliasChoices("walletActivated")
    )
    pseudonym: str | None = None
    display_username_public: bool | None = Field(
        default=None,
        validation_alias=AliasChoices("displayUsernamePublic"),
    )
    profile_image: str | None = Field(
        default=None, validation_alias=AliasChoices("profileImage")
    )
    bio: str | None = None
    proxy_wallet: str | None = Field(
        default=None, validation_alias=AliasChoices("proxyWallet")
    )


class SearchResults(BaseModel):
    model_config = ConfigDict(extra="ignore")

    profiles: list[SearchProfile] = Field(default_factory=list)
    pagination: SearchPagination | None = None


class ActivityRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    proxy_wallet: str = Field(validation_alias=AliasChoices("proxyWallet"))
    timestamp: int
    condition_id: str = Field(validation_alias=AliasChoices("conditionId"))
    type: ActivityKind | str = ActivityKind.TRADE
    size: float = 0.0
    usdc_size: float | None = Field(
        default=None, validation_alias=AliasChoices("usdcSize")
    )
    transaction_hash: str | None = Field(
        default=None, validation_alias=AliasChoices("transactionHash")
    )
    price: float | None = None
    asset: str | None = None
    side: TradeSide | None = None
    outcome_index: int | None = Field(
        default=None, validation_alias=AliasChoices("outcomeIndex")
    )
    title: str | None = None
    slug: str | None = None
    icon: str | None = None
    event_id: str | None = Field(default=None, validation_alias=AliasChoices("eventId"))
    event_slug: str | None = Field(
        default=None, validation_alias=AliasChoices("eventSlug")
    )
    outcome: str | None = None
    name: str | None = None
    pseudonym: str | None = None
    bio: str | None = None
    profile_image: str | None = Field(
        default=None, validation_alias=AliasChoices("profileImage")
    )
    profile_image_optimized: str | None = Field(
        default=None,
        validation_alias=AliasChoices("profileImageOptimized"),
    )


class PositionSnapshot(BaseModel):
    model_config = ConfigDict(extra="ignore")

    proxy_wallet: str = Field(validation_alias=AliasChoices("proxyWallet"))
    asset: str
    condition_id: str = Field(validation_alias=AliasChoices("conditionId"))
    size: float = 0.0
    avg_price: float | None = Field(
        default=None, validation_alias=AliasChoices("avgPrice")
    )
    initial_value: float | None = Field(
        default=None, validation_alias=AliasChoices("initialValue")
    )
    current_value: float | None = Field(
        default=None, validation_alias=AliasChoices("currentValue")
    )
    cash_pnl: float | None = Field(
        default=None, validation_alias=AliasChoices("cashPnl")
    )
    percent_pnl: float | None = Field(
        default=None, validation_alias=AliasChoices("percentPnl")
    )
    total_bought: float | None = Field(
        default=None, validation_alias=AliasChoices("totalBought")
    )
    realized_pnl: float | None = Field(
        default=None, validation_alias=AliasChoices("realizedPnl")
    )
    percent_realized_pnl: float | None = Field(
        default=None,
        validation_alias=AliasChoices("percentRealizedPnl"),
    )
    cur_price: float | None = Field(
        default=None, validation_alias=AliasChoices("curPrice")
    )
    redeemable: bool | None = None
    mergeable: bool | None = None
    title: str | None = None
    slug: str | None = None
    icon: str | None = None
    event_id: str | None = Field(default=None, validation_alias=AliasChoices("eventId"))
    event_slug: str | None = Field(
        default=None, validation_alias=AliasChoices("eventSlug")
    )
    outcome: str | None = None
    outcome_index: int | None = Field(
        default=None, validation_alias=AliasChoices("outcomeIndex")
    )
    opposite_outcome: str | None = Field(
        default=None, validation_alias=AliasChoices("oppositeOutcome")
    )
    opposite_asset: str | None = Field(
        default=None, validation_alias=AliasChoices("oppositeAsset")
    )
    end_date: str | None = Field(default=None, validation_alias=AliasChoices("endDate"))
    negative_risk: bool | None = Field(
        default=None, validation_alias=AliasChoices("negativeRisk")
    )


class ClosedPositionSnapshot(BaseModel):
    model_config = ConfigDict(extra="ignore")

    proxy_wallet: str = Field(validation_alias=AliasChoices("proxyWallet"))
    asset: str
    condition_id: str = Field(validation_alias=AliasChoices("conditionId"))
    avg_price: float | None = Field(
        default=None, validation_alias=AliasChoices("avgPrice")
    )
    total_bought: float | None = Field(
        default=None, validation_alias=AliasChoices("totalBought")
    )
    realized_pnl: float | None = Field(
        default=None, validation_alias=AliasChoices("realizedPnl")
    )
    cur_price: float | None = Field(
        default=None, validation_alias=AliasChoices("curPrice")
    )
    timestamp: int | None = None
    title: str | None = None
    slug: str | None = None
    icon: str | None = None
    event_slug: str | None = Field(
        default=None, validation_alias=AliasChoices("eventSlug")
    )
    outcome: str | None = None
    outcome_index: int | None = Field(
        default=None, validation_alias=AliasChoices("outcomeIndex")
    )
    opposite_outcome: str | None = Field(
        default=None, validation_alias=AliasChoices("oppositeOutcome")
    )
    opposite_asset: str | None = Field(
        default=None, validation_alias=AliasChoices("oppositeAsset")
    )
    end_date: str | None = Field(default=None, validation_alias=AliasChoices("endDate"))


class TotalValueSnapshot(BaseModel):
    model_config = ConfigDict(extra="ignore")

    user: str
    value: float


class EventTag(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    label: str | None = None
    slug: str | None = None


class EventCategory(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    label: str | None = None
    slug: str | None = None


class EventMarket(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    question: str | None = None
    condition_id: str = Field(validation_alias=AliasChoices("conditionId"))
    slug: str | None = None
    description: str | None = None
    category: str | None = None
    market_type: str | None = Field(
        default=None, validation_alias=AliasChoices("marketType")
    )
    sports_market_type: str | None = Field(
        default=None, validation_alias=AliasChoices("sportsMarketType")
    )
    outcomes: list[str] = Field(default_factory=list)
    outcome_prices: list[float] = Field(
        default_factory=list, validation_alias=AliasChoices("outcomePrices")
    )
    clob_token_ids: list[str] = Field(
        default_factory=list, validation_alias=AliasChoices("clobTokenIds")
    )
    last_trade_price: float | None = Field(
        default=None, validation_alias=AliasChoices("lastTradePrice")
    )
    best_bid: float | None = Field(
        default=None, validation_alias=AliasChoices("bestBid")
    )
    best_ask: float | None = Field(
        default=None, validation_alias=AliasChoices("bestAsk")
    )

    @field_validator("outcomes", mode="before")
    @classmethod
    def _parse_outcomes(cls, value: Any) -> list[str]:
        return [str(item) for item in _parse_json_list(value)]

    @field_validator("outcome_prices", mode="before")
    @classmethod
    def _parse_prices(cls, value: Any) -> list[float]:
        parsed = _parse_json_list(value)
        prices: list[float] = []
        for item in parsed:
            try:
                prices.append(float(item))
            except (TypeError, ValueError):
                continue
        return prices

    @field_validator("clob_token_ids", mode="before")
    @classmethod
    def _parse_token_ids(cls, value: Any) -> list[str]:
        return [str(item) for item in _parse_json_list(value)]


class EventMetadata(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    slug: str | None = None
    title: str | None = None
    description: str | None = None
    category: str | None = None
    subcategory: str | None = None
    tags: list[EventTag] = Field(default_factory=list)
    categories: list[EventCategory] = Field(default_factory=list)
    markets: list[EventMarket] = Field(default_factory=list)

    def find_market(self, condition_id: str) -> EventMarket | None:
        for market in self.markets:
            if market.condition_id == condition_id:
                return market
        return None


class ResolvedTrader(BaseModel):
    model_config = ConfigDict(extra="ignore")

    input_type: Literal["wallet", "name"]
    input_value: str
    wallet: str
    display_name: str | None = None
    profile_name: str | None = None
    pseudonym: str | None = None
    bio: str | None = None
    verified_badge: bool = False
    created_at: datetime | None = None
    profile_found: bool = False


class NormalizedTrade(BaseModel):
    model_config = ConfigDict(extra="ignore")

    wallet: str
    timestamp: datetime
    timestamp_unix: int
    side: TradeSide
    condition_id: str
    asset_id: str
    outcome: str
    outcome_index: int | None = None
    market_title: str
    market_slug: str | None = None
    event_title: str | None = None
    event_slug: str | None = None
    event_id: str | None = None
    price: float
    quantity: float
    notional: float
    transaction_hash: str | None = None
    profile_name: str | None = None
    pseudonym: str | None = None
    canonical_topic: str = "other"
    topic_source: str = "unknown"
    official_topics: list[str] = Field(default_factory=list)
    fallback_topics: list[str] = Field(default_factory=list)
    theme_labels: list[str] = Field(default_factory=list)
    market_type: str | None = None


class OpenLot(BaseModel):
    model_config = ConfigDict(extra="ignore")

    asset_id: str
    condition_id: str
    event_slug: str | None = None
    market_title: str
    outcome: str
    canonical_topic: str
    topic_source: str
    official_topics: list[str] = Field(default_factory=list)
    market_type: str | None = None
    open_timestamp: datetime
    remaining_quantity: float
    entry_price: float


class LotMatch(BaseModel):
    model_config = ConfigDict(extra="ignore")

    asset_id: str
    condition_id: str
    event_slug: str | None = None
    market_title: str
    outcome: str
    canonical_topic: str
    close_timestamp: datetime
    open_timestamp: datetime | None = None
    quantity: float
    entry_price: float
    exit_price: float
    realized_pnl: float
    hold_seconds: float | None = None
    synthetic_cost_basis: bool = False


class RankedStat(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    trade_count: int = 0
    volume: float = 0.0
    realized_pnl: float | None = None
    unrealized_pnl: float | None = None
    total_pnl: float | None = None
    share_of_volume: float | None = None


class MarketPnL(BaseModel):
    model_config = ConfigDict(extra="ignore")

    condition_id: str
    market_title: str
    event_slug: str | None = None
    event_title: str | None = None
    canonical_topic: str = "other"
    official_topics: list[str] = Field(default_factory=list)
    market_type: str | None = None
    trade_count: int = 0
    volume: float = 0.0
    buy_count: int = 0
    sell_count: int = 0
    yes_trade_count: int = 0
    no_trade_count: int = 0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_pnl: float = 0.0
    open_quantity: float = 0.0
    share_of_volume: float = 0.0


class WinLossRate(BaseModel):
    model_config = ConfigDict(extra="ignore")

    wins: int = 0
    losses: int = 0
    flat: int = 0
    win_rate: float = 0.0
    loss_rate: float = 0.0


class HeatmapBucket(BaseModel):
    model_config = ConfigDict(extra="ignore")

    day_of_week: str
    day_index: int
    hour_utc: int
    trade_count: int
    volume: float


class BehaviorMetrics(BaseModel):
    model_config = ConfigDict(extra="ignore")

    buy_count: int = 0
    sell_count: int = 0
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    buy_sell_ratio: float | None = None
    yes_count: int = 0
    no_count: int = 0
    yes_volume: float = 0.0
    no_volume: float = 0.0
    yes_preference_ratio: float | None = None
    average_entry_price: float | None = None
    average_exit_price: float | None = None
    average_position_size: float | None = None
    median_position_size: float | None = None
    average_hold_seconds: float | None = None
    favorite_topics: list[str] = Field(default_factory=list)
    favorite_market_types: list[str] = Field(default_factory=list)
    peak_hours_utc: list[int] = Field(default_factory=list)
    peak_days_utc: list[str] = Field(default_factory=list)
    top_market_concentration: float = 0.0
    top_event_concentration: float = 0.0
    top_category_concentration: float = 0.0


class OfficialSnapshots(BaseModel):
    model_config = ConfigDict(extra="ignore")

    total_value: float | None = None
    current_positions_value: float | None = None
    current_positions_cash_pnl: float | None = None
    current_positions_realized_pnl: float | None = None
    closed_positions_realized_pnl: float | None = None


class AnalysisReport(BaseModel):
    model_config = ConfigDict(extra="ignore")

    trader: ResolvedTrader
    analysis_start: datetime | None = None
    analysis_end: datetime
    trade_count_total_history: int = 0
    trade_count_in_window: int = 0
    total_volume: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_pnl: float = 0.0
    distinct_markets: int = 0
    win_loss: WinLossRate = Field(default_factory=WinLossRate)
    top_categories: list[RankedStat] = Field(default_factory=list)
    top_themes: list[RankedStat] = Field(default_factory=list)
    top_market_types: list[RankedStat] = Field(default_factory=list)
    top_markets_by_count: list[MarketPnL] = Field(default_factory=list)
    top_markets_by_volume: list[MarketPnL] = Field(default_factory=list)
    market_pnls: list[MarketPnL] = Field(default_factory=list)
    best_markets_by_realized_pnl: list[MarketPnL] = Field(default_factory=list)
    worst_markets_by_realized_pnl: list[MarketPnL] = Field(default_factory=list)
    most_profitable_category: RankedStat | None = None
    least_profitable_category: RankedStat | None = None
    behavior: BehaviorMetrics = Field(default_factory=BehaviorMetrics)
    heatmap: list[HeatmapBucket] = Field(default_factory=list)
    summary: str = ""
    assumptions: list[str] = Field(default_factory=list)
    official_snapshots: OfficialSnapshots | None = None
    normalized_trades: list[NormalizedTrade] = Field(default_factory=list)
