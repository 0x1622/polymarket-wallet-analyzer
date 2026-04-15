# Polymarket Wallet Analyzer

`Polymarket Wallet Analyzer` is a Python 3.11+ CLI for analyzing a public Polymarket trader by wallet address or public profile name.

It uses Polymarket's public APIs first and does not require authenticated CLOB access for normal analysis.

## What It Reports

- overall profit/loss
- realized PnL
- unrealized PnL
- total volume traded
- win/loss rate by market
- most traded categories and themes
- trade behavior patterns
- a plain-English summary of what the account trades and how it trades

Behavior analysis includes:

- average hold time
- average entry price
- average exit price
- buys vs sells ratio
- YES vs NO preference
- favorite market types and topics
- UTC time-of-day and day-of-week activity
- concentration by market, event, and category

## Data Sources

The implementation is built around public Polymarket endpoints documented in the official docs.

Data API:

- `/activity`
- `/positions`
- `/closed-positions`
- `/value`

Gamma API:

- `/public-profile`
- `/public-search`
- `/events/slug/{slug}`

No API key is required for public trader analysis.

## Project Layout

- `client.py`: reusable `PolymarketClient`, retry/backoff, pagination, metadata caching
- `resolver.py`: wallet/name resolution logic and ambiguity handling
- `models.py`: pydantic models for API payloads and analysis output
- `analytics.py`: pure normalization, FIFO PnL engine, category inference, behavior analytics
- `cli.py`: argparse entrypoint and `rich` terminal rendering
- `utils.py`: shared helpers, formatting, CSV export, and custom exceptions
- `example_output.txt`: captured sample CLI output from a live run

## Requirements

- Python 3.11+
- `httpx`
- `pydantic`
- `rich`

Install in a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Quick Start

Analyze a wallet:

```bash
.venv/bin/python cli.py --wallet 0xABC...
```

Analyze a public profile name:

```bash
.venv/bin/python cli.py --name some_trader
```

Analyze a bounded date range:

```bash
.venv/bin/python cli.py --wallet 0xABC... --from-date 2025-01-01 --to-date 2026-01-01
```

Emit JSON instead of terminal tables:

```bash
.venv/bin/python cli.py --name some_trader --json
```

Export normalized trades and market PnL to CSV:

```bash
.venv/bin/python cli.py --wallet 0xABC... --csv-out trader_analysis.csv
```

This writes:

- `trader_analysis_trades.csv`
- `trader_analysis_markets.csv`

Enable verbose logging:

```bash
.venv/bin/python cli.py --wallet 0xABC... --verbose
```

## CLI Reference

```text
--wallet 0x...                  Analyze by wallet address
--name PROFILE_NAME            Analyze by public profile/account name
--from-date YYYY-MM-DD         Filter report metrics from this UTC date
--to-date YYYY-MM-DD           Filter report metrics through this UTC date
--json                         Emit machine-readable JSON
--csv-out output.csv           Export normalized trades and market PnL CSVs
--top N                        Number of rows shown in top tables
--api-key                      Optional future-proof auth parameter
--api-secret                   Optional future-proof auth parameter
--api-passphrase               Optional future-proof auth parameter
--verbose                      Enable verbose logging
```

Exactly one of `--wallet` or `--name` must be provided.

## Name Resolution

When `--wallet` is provided, the wallet is used directly.

When `--name` is provided:

1. the tool searches Polymarket public profiles through Gamma `/public-search`
2. it scores the returned candidates against the requested name
3. if one match is clearly better than the others, it resolves to that wallet
4. if multiple matches are plausible, it fails safely and prints the candidate list

This avoids silently analyzing the wrong account.

## Output Modes

Default terminal output uses `rich` tables and panels and includes:

- trader identity summary
- date range analyzed
- total trades analyzed
- realized PnL
- unrealized PnL
- total PnL
- total notional volume
- distinct markets traded
- top categories
- top markets by volume
- behavior metrics
- a plain-English summary
- explicit PnL assumptions

JSON output contains the full analysis payload, including:

- normalized trades
- per-market PnL
- top categories, themes, and market types
- behavior metrics
- heatmap buckets
- assumptions
- public snapshot values from `/positions`, `/closed-positions`, and `/value`

## PnL Methodology

The tool reconstructs trades chronologically and applies FIFO cost basis per outcome token asset.

Core rules:

1. BUY trades open FIFO lots for the exact outcome token asset.
2. SELL trades close earlier BUY lots for the same asset.
3. Realized PnL is recognized on matched close lots.
4. Unrealized PnL is computed from remaining open lots using the best available public mark.
5. Total PnL is `realized + unrealized`.

Marking logic:

1. current position mark prices from public `/positions` are preferred
2. if no current mark exists for an asset, the latest public Gamma event market price is used

Important assumptions and edge cases:

1. partial closes are supported
2. multiple buys before a sell are matched oldest-first
3. YES and NO are tracked independently per asset and then aggregated back to the market level
4. if the API history contains a SELL without enough prior BUY inventory, the unmatched quantity is assigned a synthetic cost basis equal to the sell price so profit is not overstated
5. when `--from-date` is used, earlier trades are still loaded to seed FIFO cost basis, but only markets touched in the analyzed window are reported
6. public `/positions` and `/value` are current snapshots; if you analyze a historical `--to-date`, unrealized values still depend on the latest available public pricing because Polymarket does not expose public historical end-of-day position snapshots through these endpoints

## Category and Theme Inference

Topic inference uses official Gamma metadata first.

Preferred sources:

- event `category`
- event `subcategory`
- event `categories[]`
- event `tags[]`
- market `category`
- market `marketType`
- market `sportsMarketType`

If official metadata is sparse, the fallback classifier in `analytics.py` maps titles, slugs, descriptions, and tags into canonical buckets such as:

- `elections`
- `politics`
- `crypto`
- `sports`
- `macro`
- `tech`
- `geopolitics`
- `business`
- `science`
- `culture`

The mapping is intentionally lightweight and readable so it can be tuned later.

## Engineering Notes

The code is separated so fetching and analytics remain independent:

- network code lives in `client.py`
- pure analysis logic lives in `analytics.py`
- report models live in `models.py`

Performance notes:

- event metadata enrichment is fetched concurrently to keep large-wallet analysis practical
- paginated position and closed-position snapshots are fetched in concurrent batches
- market/topic inference is cached per market during normalization so repeated trades do not recompute the same enrichment work
- the historical activity loader follows the live Data API offset behavior, which currently caps historical activity pagination below the broader generic endpoint limit shown in the spec

This makes the analytics layer easier to test and easier to extend later for:

- trader-to-trader comparisons
- richer historical pricing inputs
- authenticated endpoints if they become useful
- formal unit tests around pure functions

## Live Example

After installing dependencies, you can run:

```bash
.venv/bin/python cli.py --wallet 0x797D27B97F43429EC737f88B841e087c5C0D8298
```

A sample live output is included in `example_output.txt`.

<img width="2736" height="1792" alt="demo" src="https://github.com/user-attachments/assets/770a6b80-2f9c-4bc6-b3b0-d946c5fb6b91" />



## Current Limitations

- public APIs do not provide full historical position snapshots for exact past dates
- name resolution is intentionally conservative and will stop on ambiguous matches
- the topic classifier is heuristic when official metadata is incomplete
- there is not yet an automated test suite in this repository

## Publishing Notes

This repository is ready to be published as `polymarket-wallet-analyzer` on GitHub.
