from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from analytics import analyze_trader, normalize_trades
from client import PolymarketClient
from models import ApiCredentials, AnalysisReport, MarketPnL, RankedStat
from resolver import ProfileResolver
from utils import (
    AmbiguousResolutionError,
    NoTradesFoundError,
    PolymarketError,
    ProgressCallback,
    UTC,
    configure_logging,
    end_of_day_utc,
    export_csvs,
    format_money,
    format_percent,
    humanize_duration,
    parse_iso_date,
    start_of_day_utc,
    utc_now,
)

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table


console = Console()


class CliProgressDisplay:
    """Render pipeline progress for interactive CLI runs."""

    TOTAL_STEPS = 8

    def __init__(self, console: Console, enabled: bool) -> None:
        self.console = console
        self.enabled = enabled
        self.progress: Progress | None = None
        self.pipeline_task_id: TaskID | None = None
        self.stage_task_id: TaskID | None = None

    def __enter__(self) -> "CliProgressDisplay":
        if not self.enabled:
            return self
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=None),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=self.console,
            transient=True,
        )
        self.progress.start()
        self.pipeline_task_id = self.progress.add_task(
            "Pipeline", total=self.TOTAL_STEPS
        )
        self.stage_task_id = self.progress.add_task("Starting", total=None)
        return self

    def __exit__(self, *_: object) -> None:
        if self.progress is not None:
            self.progress.stop()

    def start_stage(self, description: str, total: int | None = None) -> None:
        if self.progress is None or self.stage_task_id is None:
            return
        self.progress.update(
            self.stage_task_id,
            description=description,
            total=total,
            completed=0,
            visible=True,
        )

    def update_stage(
        self,
        completed: int | None = None,
        total: int | None = None,
        description: str | None = None,
    ) -> None:
        if self.progress is None or self.stage_task_id is None:
            return
        update_kwargs: dict[str, object] = {}
        if completed is not None:
            update_kwargs["completed"] = completed
        if total is not None:
            update_kwargs["total"] = total
        if description is not None:
            update_kwargs["description"] = description
        if update_kwargs:
            self.progress.update(self.stage_task_id, **update_kwargs)

    def complete_stage(self, description: str | None = None) -> None:
        if self.progress is None or self.stage_task_id is None:
            return
        stage_task = self.progress.tasks[self.stage_task_id]
        total = int(stage_task.total or max(stage_task.completed, 1))
        self.progress.update(
            self.stage_task_id,
            description=description or stage_task.description,
            total=total,
            completed=total,
        )
        if self.pipeline_task_id is not None:
            self.progress.advance(self.pipeline_task_id, 1)

    def callback(self) -> ProgressCallback:
        def _callback(
            completed: int | None,
            total: int | None,
            description: str | None,
        ) -> None:
            self.update_stage(completed=completed, total=total, description=description)

        return _callback


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze a Polymarket trader by wallet or profile name."
    )
    parser.add_argument("--wallet", help="Trader wallet address (0x...).")
    parser.add_argument("--name", help="Public Polymarket profile/account name.")
    parser.add_argument(
        "--from-date", help="Filter report metrics from this UTC date (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--to-date", help="Filter report metrics through this UTC date (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of rich tables.",
    )
    parser.add_argument(
        "--csv-out",
        help="Write normalized trades and per-market PnL CSVs using this file stem.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="How many rows to show in top tables. Default: 10.",
    )
    parser.add_argument(
        "--api-key",
        help="Optional future-proof API key (not required for public analytics).",
    )
    parser.add_argument("--api-secret", help="Optional future-proof API secret.")
    parser.add_argument(
        "--api-passphrase", help="Optional future-proof API passphrase."
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose logging."
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.verbose)

    try:
        analysis_start = start_of_day_utc(parse_iso_date(args.from_date))
        analysis_end = end_of_day_utc(parse_iso_date(args.to_date)) or utc_now()
        if analysis_start and analysis_start > analysis_end:
            raise PolymarketError("--from-date cannot be later than --to-date.")

        credentials = ApiCredentials(
            api_key=args.api_key,
            api_secret=args.api_secret,
            api_passphrase=args.api_passphrase,
        )

        progress_enabled = not args.json and console.is_terminal
        with CliProgressDisplay(console=console, enabled=progress_enabled) as progress:
            with PolymarketClient(credentials=credentials) as client:
                resolver = ProfileResolver(client)

                progress.start_stage("Resolving trader", total=1)
                trader = resolver.resolve(wallet=args.wallet, name=args.name)
                progress.complete_stage(
                    f"Resolved trader: {trader.display_name or trader.wallet}"
                )

                history_end_ts = int(analysis_end.astimezone(UTC).timestamp())
                progress.start_stage("Loading trade activity")
                activities = client.get_user_activity(
                    trader.wallet,
                    end_ts=history_end_ts,
                    activity_types=("TRADE",),
                    progress_callback=progress.callback(),
                )
                progress.complete_stage(
                    f"Loaded trade activity ({len(activities):,} trades)"
                )
                if not activities:
                    raise NoTradesFoundError(
                        "No public trade activity was found for this trader."
                    )

                report_activities = [
                    item
                    for item in activities
                    if (
                        analysis_start is None
                        or item.timestamp >= int(analysis_start.timestamp())
                    )
                    and item.timestamp <= history_end_ts
                ]
                if not report_activities:
                    raise NoTradesFoundError(
                        "No trades matched the requested date range."
                    )

                metadata_slugs = sorted(
                    {item.event_slug for item in report_activities if item.event_slug}
                )
                progress.start_stage(
                    "Fetching event metadata", total=len(metadata_slugs) or 1
                )
                metadata_by_event_slug = client.get_events_by_slug(
                    metadata_slugs,
                    progress_callback=progress.callback(),
                )
                progress.complete_stage(
                    f"Fetched event metadata ({len(metadata_by_event_slug):,} events)"
                )

                progress.start_stage("Normalizing trades", total=len(activities) or 1)
                normalized_trades = normalize_trades(
                    activities,
                    metadata_by_event_slug,
                    progress_callback=progress.callback(),
                )
                progress.complete_stage(
                    f"Normalized trades ({len(normalized_trades):,} records)"
                )

                progress.start_stage("Loading current positions")
                positions = client.get_current_positions(
                    trader.wallet,
                    progress_callback=progress.callback(),
                )
                progress.complete_stage(
                    f"Loaded current positions ({len(positions):,} rows)"
                )

                progress.start_stage("Loading closed positions")
                closed_positions = client.get_closed_positions(
                    trader.wallet,
                    progress_callback=progress.callback(),
                )
                progress.complete_stage(
                    f"Loaded closed positions ({len(closed_positions):,} rows)"
                )

                progress.start_stage("Loading total value snapshot", total=1)
                total_value = client.get_total_value(trader.wallet)
                progress.complete_stage("Loaded total value snapshot")
                closed_positions_realized_pnl = sum(
                    (item.realized_pnl or 0.0) for item in closed_positions
                )

                progress.start_stage("Running analytics", total=1)
                report = analyze_trader(
                    trader=trader,
                    trades=normalized_trades,
                    positions=positions,
                    metadata_by_event_slug=metadata_by_event_slug,
                    total_value=total_value,
                    analysis_start=analysis_start,
                    analysis_end=analysis_end,
                    top_n=max(args.top, 1),
                    closed_positions_realized_pnl=closed_positions_realized_pnl,
                )
                progress.complete_stage("Analysis complete")

            if args.csv_out:
                trades_path, markets_path = export_csvs(
                    args.csv_out, report.normalized_trades, report.market_pnls
                )
                if args.json:
                    payload = report.model_dump(mode="json")
                    payload["csv_exports"] = {
                        "trades": str(trades_path),
                        "markets": str(markets_path),
                    }
                    print(json.dumps(payload, indent=2))
                    return 0
                console.print(
                    f"CSV exports written to [bold]{trades_path}[/bold] and [bold]{markets_path}[/bold]."
                )

            if args.json:
                print(json.dumps(report.model_dump(mode="json"), indent=2))
                return 0

            render_report(report, top_n=max(args.top, 1))
            return 0
    except AmbiguousResolutionError as exc:
        if args.json:
            print(
                json.dumps({"error": str(exc), "candidates": exc.candidates}, indent=2)
            )
        else:
            console.print(f"[red]{exc}[/red]")
            table = Table(title="Possible Profile Matches")
            table.add_column("Wallet")
            table.add_column("Name")
            table.add_column("Pseudonym")
            table.add_column("Bio")
            for candidate in exc.candidates:
                table.add_row(
                    candidate.get("wallet") or "",
                    candidate.get("name") or "",
                    candidate.get("pseudonym") or "",
                    candidate.get("bio") or "",
                )
            console.print(table)
        return 2
    except PolymarketError as exc:
        if args.json:
            print(json.dumps({"error": str(exc)}, indent=2))
        else:
            console.print(f"[red]{exc}[/red]")
        return 1


def render_report(report: AnalysisReport, top_n: int) -> None:
    title = report.trader.display_name or report.trader.wallet
    date_range = (
        f"{report.analysis_start.date().isoformat()} to {report.analysis_end.date().isoformat()}"
        if report.analysis_start
        else f"through {report.analysis_end.date().isoformat()}"
    )
    identity_lines = [
        f"Wallet: {report.trader.wallet}",
        f"Profile: {report.trader.profile_name or report.trader.pseudonym or 'public profile not found'}",
        f"Date range: {date_range}",
        f"Trades analyzed: {report.trade_count_in_window} (history loaded: {report.trade_count_total_history})",
    ]
    console.print(Panel("\n".join(identity_lines), title=title, expand=False))

    overview = Table(title="Overview")
    overview.add_column("Metric")
    overview.add_column("Value", justify="right")
    overview.add_row("Realized PnL", _style_pnl(report.realized_pnl))
    overview.add_row("Unrealized PnL", _style_pnl(report.unrealized_pnl))
    overview.add_row("Total PnL", _style_pnl(report.total_pnl))
    overview.add_row("Total Volume", format_money(report.total_volume))
    overview.add_row("Distinct Markets", str(report.distinct_markets))
    overview.add_row(
        "Market Win/Loss",
        f"{report.win_loss.wins}W / {report.win_loss.losses}L / {report.win_loss.flat}F",
    )
    overview.add_row("Win Rate", format_percent(report.win_loss.win_rate))
    console.print(overview)

    console.print(_ranked_table("Top Categories", report.top_categories[:top_n]))
    console.print(
        _market_table("Top Markets By Volume", report.top_markets_by_volume[:top_n])
    )

    patterns = Table(title="Behavior Patterns")
    patterns.add_column("Pattern")
    patterns.add_column("Value", justify="right")
    patterns.add_row(
        "Average hold time", humanize_duration(report.behavior.average_hold_seconds)
    )
    patterns.add_row(
        "Average entry price", _format_price(report.behavior.average_entry_price)
    )
    patterns.add_row(
        "Average exit price", _format_price(report.behavior.average_exit_price)
    )
    patterns.add_row("Buy vs sell ratio", _format_ratio(report.behavior.buy_sell_ratio))
    patterns.add_row(
        "YES preference", format_percent(report.behavior.yes_preference_ratio)
    )
    patterns.add_row(
        "Average trade size", format_money(report.behavior.average_position_size or 0.0)
    )
    patterns.add_row(
        "Median trade size", format_money(report.behavior.median_position_size or 0.0)
    )
    patterns.add_row(
        "Favorite topics", ", ".join(report.behavior.favorite_topics) or "n/a"
    )
    patterns.add_row(
        "Favorite market types",
        ", ".join(report.behavior.favorite_market_types) or "n/a",
    )
    patterns.add_row(
        "Peak UTC hours",
        ", ".join(f"{hour:02d}:00" for hour in report.behavior.peak_hours_utc) or "n/a",
    )
    patterns.add_row("Peak UTC days", ", ".join(report.behavior.peak_days_utc) or "n/a")
    patterns.add_row(
        "Top market concentration",
        format_percent(report.behavior.top_market_concentration),
    )
    patterns.add_row(
        "Top event concentration",
        format_percent(report.behavior.top_event_concentration),
    )
    patterns.add_row(
        "Top category concentration",
        format_percent(report.behavior.top_category_concentration),
    )
    console.print(patterns)

    console.print(Panel(report.summary, title="Behavior Summary", expand=False))
    console.print(
        Panel(
            "\n".join(f"- {item}" for item in report.assumptions),
            title="PnL Assumptions",
            expand=False,
        )
    )


def _style_pnl(value: float) -> str:
    style = "green" if value >= 0 else "red"
    return f"[{style}]{format_money(value)}[/{style}]"


def _ranked_table(title: str, stats: Sequence[RankedStat]) -> Table:
    table = Table(title=title)
    table.add_column("Name")
    table.add_column("Trades", justify="right")
    table.add_column("Volume", justify="right")
    table.add_column("Share", justify="right")
    table.add_column("Total PnL", justify="right")
    for item in stats:
        total_pnl = (
            format_money(item.total_pnl) if item.total_pnl is not None else "n/a"
        )
        table.add_row(
            item.name,
            str(item.trade_count),
            format_money(item.volume),
            format_percent(item.share_of_volume),
            total_pnl,
        )
    return table


def _market_table(title: str, markets: Sequence[MarketPnL]) -> Table:
    table = Table(title=title)
    table.add_column("Market")
    table.add_column("Trades", justify="right")
    table.add_column("Volume", justify="right")
    table.add_column("Realized", justify="right")
    table.add_column("Unrealized", justify="right")
    table.add_column("Total", justify="right")
    for market in markets:
        table.add_row(
            market.market_title,
            str(market.trade_count),
            format_money(market.volume),
            format_money(market.realized_pnl),
            format_money(market.unrealized_pnl),
            format_money(market.total_pnl),
        )
    return table


def _format_price(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4f}"


def _format_ratio(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}:1"


if __name__ == "__main__":
    sys.exit(main())
