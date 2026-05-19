# Polymarket — “Nothing Ever Happens” tooling

This folder is separate from the ArbitrageLab notebook research in the rest of the repository. It holds utilities related to [Polymarket](https://polymarket.com/) and the **Nothing Ever Happens** (NEH) strategy described in internal notes (emotional categories, longshot YES / contrarian NO framing, maker-style execution, diversification).

## Categories (Gamma tag slugs)

Aligned with the NEH browse order from research notes:

| Order | NEH category   | Polymarket tag slug (default) | Notes |
|------:|----------------|------------------------------|--------|
| 1     | Sports         | `sports`                     | |
| 2     | Crypto         | `crypto`                     | |
| 3     | Politics       | `politics`                   | |
| 4     | Entertainment  | `pop-culture`                | Gamma’s `entertainment` slug currently returns no active events; `pop-culture` maps to the **Culture** tag. |
| 5     | Weather        | `weather`                    | |
| 6     | Media          | `breaking-news`            | Gamma’s `media` tag is empty in practice; `breaking-news` is a workable headline-driven proxy. |
| 7     | World Events   | `geopolitics`                | |

Override defaults entirely with `--tag-slugs`, or add more with `--extra-tag-slugs` (for example `world` or `celebrities`).

## Install

```bash
pip install -r Polymarket/requirements.txt
```

## Run (WebSocket — default, lowest latency)

```bash
python Polymarket/neh_realtime_trades.py
```

## Run (HTTP polling — no extra dependencies)

Uses the public [Data API](https://data-api.polymarket.com) `GET /trades` feed and filters to markets discovered under the same tags. Illiquid markets may not appear in the most recent trades window; prefer WebSocket when possible.

```bash
python Polymarket/neh_realtime_trades.py --transport poll
```

Press Ctrl+C to stop.
