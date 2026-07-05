#!/usr/bin/env python3
"""
Loaf Markets multi-property market-making bot.

Implements the "Strategy sketch" from the Loaf "Building a trading bot" guide,
applied across EVERY property currently live on the exchange:
  1. Discover all LIVE properties via GET /api/trade (public, no auth).
  2. For each property, poll its order book via GET /api/trade/:tokenName.
  3. Compute a quote: best bid + tick (our bid), best ask - tick (our ask),
     sized to a max inventory cap, tick expressed as a % of mid price so it
     scales sensibly across wildly different price levels.
  4. Reconcile our resting orders (fetched once per pass) against the book,
     per property.
  5. Replace stale quotes: cancel -> new nonce -> place.
  6. Skip a property for this cycle on risk events: wide spread, missing
     book side, or success=false from the API. One property's risk event
     never blocks the others.

This script runs an internal loop for a bounded wall-clock duration
(RUN_DURATION_SECONDS) and then exits cleanly. It is designed to be invoked
repeatedly by a GitHub Actions schedule (see .github/workflows/trading-bot.yml),
since GitHub Actions cannot run a single job forever.

IMPORTANT:
- LOAF_API_BASE defaults to https://api.loafmarkets.com (production). Point
  it at a dev/staging host instead while testing, if one is available to you.
- Trading ALL live properties multiplies API calls roughly by the property
  count every cycle. Keep TICK_SLEEP_SECONDS reasonable and watch your rate
  limits / logs, especially right after enabling this.
- This is example/reference code, not financial advice. Trading involves
  risk of loss. Test with small size and short RUN_DURATION_SECONDS first.
"""

import os
import sys
import time
import logging
import secrets
import requests

# --------------------------------------------------------------------------
# Configuration (all from environment variables / GitHub Secrets)
# --------------------------------------------------------------------------

LOAF_API_BASE = os.environ.get("LOAF_API_BASE", "https://api.loafmarkets.com")
LOAF_API_KEY = os.environ["LOAF_API_KEY"]  # required, no default on purpose

# Comma-separated tokenNames to restrict trading to (e.g. "opera,eiffel").
# Leave empty / unset to trade EVERY property with status == "LIVE",
# discovered fresh from GET /api/trade on every pass.
_allowlist_raw = os.environ.get("PROPERTY_ALLOWLIST", "").strip()
PROPERTY_ALLOWLIST = (
    {t.strip() for t in _allowlist_raw.split(",") if t.strip()}
    if _allowlist_raw
    else None
)

# Strategy knobs (applied per-property). Tick and requote tolerance are
# expressed as a PERCENTAGE of mid price rather than a fixed absolute value,
# because price scale varies enormously across properties (e.g. ~$97 for
# "rainier" vs ~$1189 for "liberty").
TICK_SIZE_PCT = float(os.environ.get("TICK_SIZE_PCT", "0.001"))       # 0.1% of mid
ORDER_SIZE = float(os.environ.get("ORDER_SIZE", "1"))
MAX_INVENTORY = float(os.environ.get("MAX_INVENTORY", "10"))
MAX_SPREAD_PCT = float(os.environ.get("MAX_SPREAD_PCT", "0.05"))      # 5% of mid
REQUOTE_TOLERANCE_PCT = float(os.environ.get("REQUOTE_TOLERANCE_PCT", "0.002"))
TICK_SLEEP_SECONDS = float(os.environ.get("TICK_SLEEP_SECONDS", "10"))

# Bounded run duration per invocation (GitHub Actions can't loop forever)
RUN_DURATION_SECONDS = float(os.environ.get("RUN_DURATION_SECONDS", "240"))

HEADERS = {
    "Authorization": f"Bearer {LOAF_API_KEY}",
    "Content-Type": "application/json",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("loaf-bot")

session = requests.Session()


# --------------------------------------------------------------------------
# API helpers
# --------------------------------------------------------------------------

def get_properties():
    """
    Public endpoint. Returns every property Loaf knows about, with
    marketPrice, candlesticks, etc. We filter to status == "LIVE" and,
    if PROPERTY_ALLOWLIST is set, to just those tokenNames.
    """
    r = session.get(f"{LOAF_API_BASE}/api/trade", timeout=10)
    r.raise_for_status()
    data = r.json()
    props = data.get("properties", [])
    live = [p for p in props if p.get("status") == "LIVE"]
    if PROPERTY_ALLOWLIST:
        live = [p for p in live if p.get("tokenName") in PROPERTY_ALLOWLIST]
    return live


def get_book(token_name: str):
    """
    Public endpoint, embedded order-book snapshot for a single token.

    NOTE: the exact response shape for this endpoint isn't confirmed from
    the docs alone (only the WebSocket orderbook_update shape is documented
    with bids/asks). best_bid_ask() below tries a couple of reasonable
    shapes defensively. If neither matches your real response, check the
    Action logs and adjust best_bid_ask() to match.
    """
    r = session.get(f"{LOAF_API_BASE}/api/trade/{token_name}", timeout=10)
    r.raise_for_status()
    return r.json()


def get_active_orders():
    r = session.get(
        f"{LOAF_API_BASE}/api/history/orders/active",
        headers=HEADERS,
        timeout=10,
    )
    r.raise_for_status()
    return r.json().get("activeOrders", [])


def request_nonce():
    r = session.post(
        f"{LOAF_API_BASE}/api/orders/nonce",
        headers=HEADERS,
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    return data["nonce"], data["deadline"]


def place_order(property_id: int, side: str, price: float, quantity: float):
    nonce, _deadline = request_nonce()
    body = {
        "propertyId": property_id,
        "price": round(price, 2),
        "quantity": round(quantity, 1),
        "side": side,
        "type": "LIMIT",
        "timeInForce": "GTC",
        "deadline": 0,
        "nonce": nonce,
    }
    r = session.post(
        f"{LOAF_API_BASE}/api/orders/",
        headers=HEADERS,
        json=body,
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        log.warning("[propertyId=%s] Order rejected: %s", property_id, data.get("errorMessage"))
        return None
    log.info(
        "[propertyId=%s] Placed %s %.1f @ %.2f -> orderId=%s",
        property_id, side, quantity, price, data["orderId"],
    )
    return data["orderId"]


def cancel_order(order_id: int):
    r = session.post(
        f"{LOAF_API_BASE}/api/orders/cancel",
        headers=HEADERS,
        json={"orderId": order_id},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        log.warning("Cancel failed for %s: %s", order_id, data.get("errorMessage"))
    else:
        log.info("Cancelled orderId=%s", order_id)
    return data.get("success", False)


def cancel_all():
    r = session.post(
        f"{LOAF_API_BASE}/api/orders/cancel-all",
        headers=HEADERS,
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


# --------------------------------------------------------------------------
# Strategy
# --------------------------------------------------------------------------

def best_bid_ask(book: dict):
    """
    Tries a couple of reasonable shapes for GET /api/trade/:tokenName since
    only the WebSocket orderbook_update shape is documented explicitly:
      { "bids": [{"price": ..., "quantity": ..., "orderId": ...}, ...],
        "asks": [...] }
    ...possibly nested under "orderbook". Falls back gracefully to (None, None)
    if neither shape matches, which just makes run_cycle() skip that property.
    """
    node = book
    if "bids" not in node and "asks" not in node and isinstance(book.get("orderbook"), dict):
        node = book["orderbook"]

    bids = node.get("bids") or []
    asks = node.get("asks") or []
    if not bids or not asks:
        return None, None
    best_bid = max(bids, key=lambda x: x["price"])["price"]
    best_ask = min(asks, key=lambda x: x["price"])["price"]
    return best_bid, best_ask


def run_cycle_for_property(prop: dict, active_orders: list):
    token_name = prop["tokenName"]
    property_id = prop["propertyId"]

    book = get_book(token_name)
    best_bid, best_ask = best_bid_ask(book)

    if best_bid is None or best_ask is None:
        log.warning("[%s] Risk event: missing book side, skipping this cycle.", token_name)
        return

    mid = (best_bid + best_ask) / 2
    spread = best_ask - best_bid
    spread_pct = spread / mid if mid else float("inf")

    if spread_pct > MAX_SPREAD_PCT:
        log.warning(
            "[%s] Risk event: spread too wide (%.2f%% > %.2f%%), skipping this cycle.",
            token_name, spread_pct * 100, MAX_SPREAD_PCT * 100,
        )
        return

    tick = mid * TICK_SIZE_PCT
    requote_tolerance = mid * REQUOTE_TOLERANCE_PCT

    our_bid_price = best_bid + tick
    our_ask_price = best_ask - tick
    if our_bid_price >= our_ask_price:
        # Book is too tight to quote inside without crossing; join instead.
        our_bid_price = best_bid
        our_ask_price = best_ask

    our_open = [o for o in active_orders if o.get("propertyId") == property_id]
    current_bid = next((o for o in our_open if o.get("side") == "BUY"), None)
    current_ask = next((o for o in our_open if o.get("side") == "SELL"), None)

    net_inventory = sum(
        o["quantity"] if o.get("side") == "BUY" else -o["quantity"]
        for o in our_open
    )

    # --- Bid side ---
    if current_bid is None or abs(current_bid["price"] - our_bid_price) > requote_tolerance:
        if current_bid is not None:
            cancel_order(current_bid["orderId"])
        if net_inventory < MAX_INVENTORY:
            place_order(property_id, "BUY", our_bid_price, ORDER_SIZE)
        else:
            log.info("[%s] Skipping bid: inventory cap reached (%.1f)", token_name, net_inventory)

    # --- Ask side ---
    if current_ask is None or abs(current_ask["price"] - our_ask_price) > requote_tolerance:
        if current_ask is not None:
            cancel_order(current_ask["orderId"])
        if net_inventory > -MAX_INVENTORY:
            place_order(property_id, "SELL", our_ask_price, ORDER_SIZE)
        else:
            log.info("[%s] Skipping ask: inventory cap reached (%.1f)", token_name, net_inventory)

    log.info(
        "[%s] Cycle done. mid=%.2f spread=%.2f (%.2f%%) inventory=%.1f",
        token_name, mid, spread, spread_pct * 100, net_inventory,
    )


def run_pass():
    """One full pass: discover live properties, fetch our open orders once,
    then run the quote/requote logic for every property in turn."""
    properties = get_properties()
    if not properties:
        log.warning("No LIVE properties found (or allowlist matched none); skipping this pass.")
        return

    try:
        active_orders = get_active_orders()
    except requests.HTTPError as e:
        log.error("Could not fetch active orders, skipping this pass: %s", e)
        return

    log.info("Pass covering %d live properties: %s",
              len(properties), ", ".join(p["tokenName"] for p in properties))

    for prop in properties:
        try:
            run_cycle_for_property(prop, active_orders)
        except requests.HTTPError as e:
            log.error("[%s] HTTP error during cycle: %s", prop.get("tokenName"), e)
        except Exception as e:  # noqa: BLE001 - one property's failure shouldn't stop the rest
            log.error("[%s] Unexpected error during cycle: %s", prop.get("tokenName"), e)


def main():
    log.info(
        "Starting bot: base=%s allowlist=%s duration=%ss",
        LOAF_API_BASE, sorted(PROPERTY_ALLOWLIST) if PROPERTY_ALLOWLIST else "ALL LIVE",
        RUN_DURATION_SECONDS,
    )

    start = time.time()
    while time.time() - start < RUN_DURATION_SECONDS:
        run_pass()
        time.sleep(TICK_SLEEP_SECONDS)

    log.info("Run duration elapsed, exiting cleanly (next scheduled run will continue).")


if __name__ == "__main__":
    main()
