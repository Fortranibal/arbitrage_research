#!/usr/bin/env python3
"""
Stream new Polymarket trades for markets under the Nothing Ever Happens (NEH)
emotional-category set (see Polymarket/README.md).

Modes
-----
* WebSocket (default): CLOB ``last_trade_price`` events on subscribed outcome
  tokens — Polymarket Market channel (``wss://ws-subscriptions-clob.polymarket.com/ws/market``).
* HTTP poll (``--transport poll``): public Data API ``GET /trades`` — no third-party
  dependencies beyond the standard library.

Discovery uses the Gamma API (``https://gamma-api.polymarket.com``) with the same
tag slugs as in the NEH notes.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"
WS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# NEH emotional categories → Polymarket Gamma tag slugs (see Polymarket/README.md).
DEFAULT_TAG_SLUGS: tuple[str, ...] = (
    "sports",
    "crypto",
    "politics",
    "pop-culture",  # NEH “Entertainment” — Gamma `entertainment` slug currently yields no active events
    "weather",
    "breaking-news",  # NEH “Media” — Gamma `media` slug is empty; headlines / attention economy proxy
    "geopolitics",  # NEH “World Events”
)


@dataclass(frozen=True)
class MarketTokens:
    condition_id: str
    question: str
    slug: str
    tag_label: str
    token_ids: tuple[str, ...]
    outcome_labels: tuple[str, ...]


def _http_get_json(url: str, timeout: float = 60.0) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "neh-realtime-trades/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _resolve_tag_slug(slug: str) -> dict[str, Any]:
    data = _http_get_json(f"{GAMMA_BASE}/tags/slug/{slug}")
    if not isinstance(data, dict) or "id" not in data:
        raise RuntimeError(f"Unexpected tag payload for slug={slug!r}: {data!r}")
    return data


def _parse_json_list_field(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        return json.loads(raw)
    raise TypeError(f"Expected list or JSON string, got {type(raw)}")


def _iter_active_markets_for_tag(tag_id: str, page_size: int, max_markets: int) -> Iterable[dict[str, Any]]:
    seen_market_ids: set[str] = set()
    offset = 0
    while len(seen_market_ids) < max_markets:
        url = (
            f"{GAMMA_BASE}/events?tag_id={tag_id}&active=true&closed=false"
            f"&limit={page_size}&offset={offset}&order=volume_24hr&ascending=false"
        )
        events = _http_get_json(url)
        if not isinstance(events, list) or not events:
            break
        for event in events:
            for m in event.get("markets") or []:
                mid = str(m.get("id", ""))
                if not mid or mid in seen_market_ids:
                    continue
                if not m.get("active") or m.get("closed"):
                    continue
                if not m.get("acceptingOrders"):
                    continue
                clob = m.get("clobTokenIds")
                if not clob:
                    continue
                try:
                    _parse_json_list_field(clob)
                except (json.JSONDecodeError, TypeError):
                    continue
                seen_market_ids.add(mid)
                yield m
                if len(seen_market_ids) >= max_markets:
                    return
        offset += page_size


def build_universe(
    tag_slugs: list[str],
    tag_page_size: int,
    max_markets_per_tag: int,
) -> tuple[dict[str, MarketTokens], dict[str, str]]:
    """
    Returns:
      asset_index: token_id -> MarketTokens
      condition_tag: condition_id (0x lower) -> human tag label (first seen wins)
    """
    asset_index: dict[str, MarketTokens] = {}
    condition_tag: dict[str, str] = {}

    for slug in tag_slugs:
        tag = _resolve_tag_slug(slug)
        tag_id = str(tag["id"])
        tag_label = str(tag.get("label") or slug)
        count = 0
        for m in _iter_active_markets_for_tag(tag_id, tag_page_size, max_markets_per_tag):
            try:
                token_ids = tuple(str(x) for x in _parse_json_list_field(m["clobTokenIds"]))
                outcomes = tuple(str(x) for x in _parse_json_list_field(m.get("outcomes") or '["Yes","No"]'))
            except (KeyError, json.JSONDecodeError, TypeError):
                continue
            cid = str(m.get("conditionId") or "").lower()
            if not cid.startswith("0x"):
                continue
            question = str(m.get("question") or m.get("groupItemTitle") or "?")
            mslug = str(m.get("slug") or "")
            mt = MarketTokens(
                condition_id=cid,
                question=question,
                slug=mslug,
                tag_label=tag_label,
                token_ids=token_ids,
                outcome_labels=outcomes if outcomes else ("Yes", "No"),
            )
            condition_tag.setdefault(cid, tag_label)
            for tid in token_ids:
                asset_index.setdefault(tid, mt)
            count += 1
        print(f"[gamma] tag {tag_label!r} ({slug}): indexed {count} markets", file=sys.stderr)

    return asset_index, condition_tag


def _fmt_ts_ms(ms: str | int | float) -> str:
    try:
        ts = float(ms) / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (TypeError, ValueError, OSError):
        return str(ms)


def _outcome_for_asset(mt: MarketTokens, asset_id: str) -> str:
    try:
        idx = mt.token_ids.index(asset_id)
    except ValueError:
        return "?"
    if idx < len(mt.outcome_labels):
        return mt.outcome_labels[idx]
    return f"outcome[{idx}]"


def _maybe_longshot_note(price: float) -> str:
    """NEH note: longshot YES band ~5–15c; optional console hint."""
    if 0.05 <= price <= 0.15:
        return "  (NEH longshot YES band)"
    return ""


def _print_trade_line(
    *,
    when: str,
    tag: str,
    question: str,
    outcome: str,
    side: str,
    price: float,
    size: float,
    condition: str,
    slug: str,
    tx: str,
    longshot_hints: bool,
) -> None:
    extra = _maybe_longshot_note(price) if longshot_hints else ""
    q = question.replace("\n", " ").strip()
    if len(q) > 120:
        q = q[:117] + "..."
    txs = tx if len(tx) <= 18 else f"{tx[:10]}…{tx[-6:]}"
    print(
        f"{when}  [{tag}]  {side} {outcome} @ {price:.4f}  size={size:.4f}  | {q}\n"
        f"           slug={slug}  condition={condition[:10]}…  tx={txs}{extra}"
    )


async def _run_websocket(
    asset_index: dict[str, MarketTokens],
    *,
    subscribe_chunk: int,
    longshot_hints: bool,
) -> None:
    try:
        import websockets
    except ImportError as e:
        raise SystemExit(
            "WebSocket mode requires the 'websockets' package. "
            "Install with:  pip install -r Polymarket/requirements.txt\n"
            "Or run with:  --transport poll"
        ) from e

    asset_ids = list(asset_index.keys())
    if not asset_ids:
        raise SystemExit("No outcome tokens in universe — check tag slugs and filters.")

    first, rest = asset_ids[:subscribe_chunk], asset_ids[subscribe_chunk:]
    print(
        f"[ws] subscribing to {len(asset_ids)} outcome tokens "
        f"(initial chunk {len(first)}, then {len(rest)} more in batches)…",
        file=sys.stderr,
    )

    async def ping_loop(ws: Any) -> None:
        while True:
            await asyncio.sleep(8.0)
            await ws.send("PING")

    async with websockets.connect(WS_MARKET, ping_interval=None) as ws:
        await ws.send(
            json.dumps(
                {
                    "assets_ids": first,
                    "type": "market",
                    "custom_feature_enabled": False,
                }
            )
        )
        ping_task = asyncio.create_task(ping_loop(ws))
        try:
            for i in range(0, len(rest), subscribe_chunk):
                chunk = rest[i : i + subscribe_chunk]
                await ws.send(json.dumps({"operation": "subscribe", "assets_ids": chunk}))
                await asyncio.sleep(0.05)
            print("[ws] connected — printing last_trade_price events (Ctrl+C to exit)\n", file=sys.stderr)
            while True:
                raw = await ws.recv()
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                s = raw.strip()
                if not s or s == "PONG":
                    continue
                if s == "[]":
                    continue
                try:
                    msg = json.loads(s)
                except json.JSONDecodeError:
                    continue
                if isinstance(msg, list):
                    items = msg
                else:
                    items = [msg]
                for obj in items:
                    if not isinstance(obj, dict):
                        continue
                    if obj.get("event_type") != "last_trade_price":
                        continue
                    aid = str(obj.get("asset_id") or "")
                    mt = asset_index.get(aid)
                    if mt is None:
                        continue
                    try:
                        price = float(obj.get("price"))
                        size = float(obj.get("size"))
                    except (TypeError, ValueError):
                        continue
                    side = str(obj.get("side") or "")
                    when = _fmt_ts_ms(str(obj.get("timestamp") or "0"))
                    outcome = _outcome_for_asset(mt, aid)
                    tx = str(obj.get("transaction_hash") or obj.get("transactionHash") or "")
                    _print_trade_line(
                        when=when,
                        tag=mt.tag_label,
                        question=mt.question,
                        outcome=outcome,
                        side=side,
                        price=price,
                        size=size,
                        condition=mt.condition_id,
                        slug=mt.slug,
                        tx=tx,
                        longshot_hints=longshot_hints,
                    )
        finally:
            ping_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ping_task


def _run_poll(
    asset_index: dict[str, MarketTokens],
    *,
    poll_interval: float,
    trades_limit: int,
    longshot_hints: bool,
) -> None:
    allowed = {mt.condition_id for mt in asset_index.values()}
    if not allowed:
        raise SystemExit("No markets in universe — check tag slugs and filters.")
    seen: set[str] = set()
    print(
        f"[poll] watching Data API /trades (limit={trades_limit}, interval={poll_interval}s)…\n",
        file=sys.stderr,
    )
    while True:
        try:
            batch = _http_get_json(f"{DATA_API_BASE}/trades?limit={trades_limit}")
        except urllib.error.URLError as e:
            print(f"[poll] request error: {e}", file=sys.stderr)
            time.sleep(poll_interval)
            continue
        if not isinstance(batch, list):
            time.sleep(poll_interval)
            continue
        for t in reversed(batch):
            if not isinstance(t, dict):
                continue
            cid = str(t.get("conditionId") or "").lower()
            if cid not in allowed:
                continue
            tx = str(t.get("transactionHash") or "")
            asset = str(t.get("asset") or "")
            ts = int(t.get("timestamp") or 0)
            side = str(t.get("side") or "")
            price = float(t.get("price") or 0)
            size = float(t.get("size") or 0)
            dedupe = f"{tx}|{asset}|{side}|{price}|{size}|{ts}"
            if dedupe in seen:
                continue
            seen.add(dedupe)
            mt = asset_index.get(asset)
            if mt is None:
                continue
            when = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            outcome = str(t.get("outcome") or _outcome_for_asset(mt, asset))
            slug = str(t.get("slug") or mt.slug)
            _print_trade_line(
                when=when,
                tag=mt.tag_label,
                question=str(t.get("title") or mt.question),
                outcome=outcome,
                side=side,
                price=price,
                size=size,
                condition=cid,
                slug=slug,
                tx=tx,
                longshot_hints=longshot_hints,
            )
        if len(seen) > 200_000:
            seen.clear()
        time.sleep(poll_interval)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--transport",
        choices=("ws", "poll"),
        default="ws",
        help="ws = CLOB WebSocket (default); poll = Data API HTTP polling (stdlib only).",
    )
    p.add_argument(
        "--tag-slugs",
        default=",".join(DEFAULT_TAG_SLUGS),
        help=f"Comma-separated Gamma tag slugs (default: NEH set).",
    )
    p.add_argument(
        "--extra-tag-slugs",
        default="",
        help="Optional extra comma-separated tag slugs (e.g. world).",
    )
    p.add_argument("--max-markets-per-tag", type=int, default=400, help="Cap markets indexed per tag.")
    p.add_argument("--gamma-page-size", type=int, default=100, help="Gamma events page size.")
    p.add_argument("--subscribe-chunk", type=int, default=80, help="WebSocket assets_ids batch size.")
    p.add_argument("--poll-interval", type=float, default=0.9, help="Seconds between /trades polls.")
    p.add_argument("--poll-limit", type=int, default=300, help="Data API trades limit per poll.")
    p.add_argument(
        "--longshot-hints",
        action="store_true",
        help="Append a note when trade price is in the rough NEH longshot YES band (5–15c).",
    )
    args = p.parse_args()
    slugs = [s.strip() for s in args.tag_slugs.split(",") if s.strip()]
    slugs.extend([s.strip() for s in args.extra_tag_slugs.split(",") if s.strip()])

    print(f"[init] tag slugs (order preserved): {', '.join(slugs)}", file=sys.stderr)
    asset_index, _ = build_universe(
        tag_slugs=slugs,
        tag_page_size=args.gamma_page_size,
        max_markets_per_tag=args.max_markets_per_tag,
    )
    n_markets = len({mt.condition_id for mt in asset_index.values()})
    print(f"[init] outcome tokens={len(asset_index)}  distinct markets≈{n_markets}", file=sys.stderr)

    if args.transport == "poll":
        _run_poll(
            asset_index,
            poll_interval=args.poll_interval,
            trades_limit=args.poll_limit,
            longshot_hints=args.longshot_hints,
        )
    else:
        asyncio.run(
            _run_websocket(
                asset_index,
                subscribe_chunk=args.subscribe_chunk,
                longshot_hints=args.longshot_hints,
            )
        )


if __name__ == "__main__":
    main()
