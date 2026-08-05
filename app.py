"""
Databricks App boilerplate:
- Serves a small Flask API
- Reads/writes to Lakebase (Databricks-managed Postgres) via lakebase.py
- Pulls data from the Massive API via massive_client.py and syncs it into Lakebase

Run locally:
    python app.py
Deploy as a Databricks App using app.yaml.
"""

import logging
import os
import re
from datetime import datetime, timezone, timedelta

import requests
from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template, request

import lakebase
from massive_client import MassiveClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("massive-app")

app = Flask(__name__)
_w = WorkspaceClient()

TABLE_NAME = os.environ.get("MASSIVE_TABLE_NAME", "massive_records")
WATCHLIST_TABLE_NAME = os.environ.get("WATCHLIST_TABLE_NAME", "watchlist")

# Basic stock ticker shape check: 1-10 uppercase letters, with an optional
# ".X" or ".XX" share-class suffix (e.g. "BRK.B"). This rejects obviously
# malformed input before we even call the Massive API.
_TICKER_RE = re.compile(r"^[A-Z]{1,10}(\.[A-Z]{1,2})?$")


def ensure_table():
    """Create the destination table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id TEXT PRIMARY KEY,
            payload JSONB NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def ensure_watchlist_table():
    """Create the watchlist table in Lakebase if it doesn't exist yet."""
    lakebase.run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {WATCHLIST_TABLE_NAME} (
            symbol TEXT NOT NULL,
            email TEXT NOT NULL,
            latest_price NUMERIC,
            price_time TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, email)
        )
        """
    )
    # For tables created before price_time existed:
    lakebase.run_write(
        f"ALTER TABLE {WATCHLIST_TABLE_NAME} ADD COLUMN IF NOT EXISTS price_time TIMESTAMPTZ"
    )


def _current_user_email() -> str:
    """
    Resolve the current user's email so the watchlist can be personalized.

    Databricks Apps inject the logged-in user's identity via the
    X-Forwarded-Email header on every request. Fall back to the Databricks
    SDK's current_user API for local development where that header isn't set.
    """
    header_email = request.headers.get("X-Forwarded-Email")
    if header_email:
        return header_email
    return _w.current_user.me().user_name


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(err):
    """Ensure all unhandled errors return JSON (not an HTML error page),
    so the frontend's resp.json() call never chokes on HTML."""
    logger.exception("Unhandled exception while processing request")
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/")
def index():
    """Simple UI to submit a list of stock symbols to sync from Massive."""
    return render_template("index.html", email=_current_user_email())


@app.route("/records")
def list_records():
    """Read records already synced into Lakebase."""
    limit = int(request.args.get("limit", 100))
    rows = lakebase.run_query(
        f"SELECT id, payload, synced_at FROM {TABLE_NAME} ORDER BY synced_at DESC LIMIT %s",
        (limit,),
    )
    return jsonify(rows)


@app.route("/sync", methods=["POST"])
def sync_from_massive():
    """
    Pull data from the Massive API (paginated, potentially huge dataset) and
    upsert it into Lakebase in batches.
    """
    ensure_table()
    client = MassiveClient()

    path = request.json.get("path", "/records") if request.is_json else "/records"
    batch_size = int(request.args.get("batch_size", 500))

    batch = []
    total = 0
    for item in client.paginated_get(path):
        batch.append(item)
        if len(batch) >= batch_size:
            total += _upsert_batch(batch)
            batch = []

    if batch:
        total += _upsert_batch(batch)

    return jsonify({"synced": total})


@app.route("/watchlist", methods=["GET"])
def get_watchlist():
    """Return the current user's watchlist symbols, with their last known price."""
    ensure_watchlist_table()
    email = _current_user_email()
    rows = lakebase.run_query(
        f"SELECT symbol, email, latest_price, price_time, updated_at FROM {WATCHLIST_TABLE_NAME} "
        f"WHERE email = %s ORDER BY symbol ASC",
        (email,),
    )
    return jsonify(rows)


@app.route("/watchlist", methods=["POST"])
def add_to_watchlist():
    """
    Fetch the latest price for a single stock symbol from Massive using
    exactly ONE API call (see MassiveClient.get_latest_price), then add/
    update that symbol on the watchlist in Lakebase.
    """
    ensure_watchlist_table()

    if request.is_json:
        symbol = request.json.get("symbol", "")
    else:
        symbol = request.form.get("symbol", "")

    symbol = symbol.strip().upper() if isinstance(symbol, str) else ""

    if not symbol or not _TICKER_RE.match(symbol):
        return jsonify({"error": f"Invalid ticker symbol: {symbol!r}"}), 400

    client = MassiveClient()
    try:
        data = client.get_latest_price(symbol)  # <-- single API call, latest price only
    except requests.HTTPError:
        # Massive returns a 404/4xx for tickers it doesn't recognize.
        return jsonify({"error": f"Unknown ticker symbol: {symbol}"}), 400

    price = _extract_latest_price(data)
    if price is None:
        # No usable price in the response (e.g. delisted/invalid ticker
        # that still 200s with an empty result set) - don't add it.
        return jsonify({"error": f"No price data available for ticker: {symbol}"}), 400

    email = _current_user_email()
    price_time = _extract_price_time(data)

    lakebase.run_write(
        f"""
        INSERT INTO {WATCHLIST_TABLE_NAME} (symbol, email, latest_price, price_time, updated_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (symbol, email) DO UPDATE
            SET latest_price = EXCLUDED.latest_price,
                price_time   = EXCLUDED.price_time,
                updated_at   = EXCLUDED.updated_at
        """,
        (symbol, email, price, price_time),
    )

    return jsonify({"symbol": symbol, "email": email, "latest_price": price})


@app.route("/watchlist", methods=["DELETE"])
def clear_watchlist():
    """Remove all of the current user's watchlist symbols."""
    ensure_watchlist_table()
    email = _current_user_email()
    deleted = lakebase.run_write(
        f"DELETE FROM {WATCHLIST_TABLE_NAME} WHERE email = %s",
        (email,),
    )
    return jsonify({"deleted": deleted})


@app.route("/watchlist/<symbol>", methods=["DELETE"])
def delete_from_watchlist(symbol):
    """Remove a single symbol from the current user's watchlist."""
    ensure_watchlist_table()

    symbol = symbol.strip().upper() if isinstance(symbol, str) else ""
    if not symbol or not _TICKER_RE.match(symbol):
        return jsonify({"error": f"Invalid ticker symbol: {symbol!r}"}), 400

    email = _current_user_email()
    deleted = lakebase.run_write(
        f"DELETE FROM {WATCHLIST_TABLE_NAME} WHERE symbol = %s AND email = %s",
        (symbol, email),
    )

    if deleted == 0:
        return jsonify({"error": f"{symbol} is not on your watchlist"}), 404

    return jsonify({"symbol": symbol, "deleted": deleted})


@app.route("/news/<symbol>", methods=["GET"])
def get_ticker_news(symbol):
    """Return news for a single ticker over the last N days (default 30)."""
    symbol = symbol.strip().upper() if isinstance(symbol, str) else ""
    if not symbol or not _TICKER_RE.match(symbol):
        return jsonify({"error": f"Invalid ticker symbol: {symbol!r}"}), 400

    days = int(request.args.get("days", 30))
    gte = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

    client = MassiveClient()
    try:
        results = client.get_news(symbol, published_gte=gte)
    except requests.HTTPError:
        return jsonify({"error": f"Could not fetch news for {symbol}"}), 400

    articles = []
    for a in results:
        desc = (a.get("description") or "").strip()
        if len(desc) > 300:
            desc = desc[:300].rstrip() + "\u2026"
        articles.append({
            "title": a.get("title"),
            "url": a.get("article_url"),
            "publisher": (a.get("publisher") or {}).get("name"),
            "published_utc": a.get("published_utc"),
            "description": desc,
            "sentiment": _news_sentiment(a.get("insights"), symbol),
        })
    return jsonify({"symbol": symbol, "days": days, "count": len(articles), "articles": articles})


@app.route("/watchlist/refresh", methods=["POST"])
def refresh_watchlist():
    """
    Re-fetch the latest price for each of the user's watchlist symbols and
    update Lakebase. Returns how many updated and which were skipped.

    Note: on the free tier with the previous-close fallback, prices only
    change once per trading day. Per-symbol failures (e.g. rate limits) are
    skipped so the rest still refresh and keep their last known price.
    """
    ensure_watchlist_table()
    email = _current_user_email()

    rows = lakebase.run_query(
        f"SELECT symbol FROM {WATCHLIST_TABLE_NAME} WHERE email = %s ORDER BY symbol ASC",
        (email,),
    )
    symbols = [r["symbol"] for r in rows]

    client = MassiveClient()
    updated, failed = 0, []
    for symbol in symbols:
        try:
            data = client.get_latest_price(symbol)
            price = _extract_latest_price(data)
        except requests.HTTPError:
            price, data = None, None
        if price is None:
            failed.append(symbol)
            continue
        lakebase.run_write(
            f"""
            UPDATE {WATCHLIST_TABLE_NAME}
               SET latest_price = %s, price_time = %s, updated_at = now()
             WHERE symbol = %s AND email = %s
            """,
            (price, _extract_price_time(data), symbol, email),
        )
        updated += 1

    return jsonify({"updated": updated, "failed": failed})


def _news_sentiment(insights, symbol):
    """Pick the sentiment for this ticker from Massive's insights array."""
    if not isinstance(insights, list):
        return None
    for ins in insights:
        if isinstance(ins, dict) and ins.get("ticker") == symbol and ins.get("sentiment"):
            return ins.get("sentiment")
    for ins in insights:  # fallback: first available
        if isinstance(ins, dict) and ins.get("sentiment"):
            return ins.get("sentiment")
    return None


def _extract_price_time(data):
    """Timestamp of the price, matching the source _extract_latest_price used."""
    if not isinstance(data, dict):
        return None
    t = data.get("ticker")
    if isinstance(t, dict):
        last = t.get("lastTrade")
        if isinstance(last, dict) and last.get("t"):
            return _epoch_to_dt(last["t"])
        mn = t.get("min")
        if isinstance(mn, dict) and mn.get("t"):
            return _epoch_to_dt(mn["t"])
        if t.get("updated"):
            return _epoch_to_dt(t["updated"])
        return None
    results = data.get("results")
    if isinstance(results, list) and results and isinstance(results[0], dict):
        return _epoch_to_dt(results[0].get("t"))
    return None


def _epoch_to_dt(value):
    """Normalize an epoch in s / ms / ns to a UTC datetime."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    if v > 1e17:      # nanoseconds
        v /= 1e9
    elif v > 1e14:    # microseconds
        v /= 1e6
    elif v > 1e11:    # milliseconds
        v /= 1e3
    try:
        return datetime.fromtimestamp(v, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _extract_latest_price(data: dict):
    """Pull a price from either the snapshot response or the previous-close
    aggregate response, preferring the most current field available."""
    if not isinstance(data, dict):
        return None

    # Snapshot shape: {"ticker": {"lastTrade":{"p":..}, "min":{"c":..},
    #                             "day":{"c":..}, "prevDay":{"c":..}}}
    t = data.get("ticker")
    if isinstance(t, dict):
        last = t.get("lastTrade")
        if isinstance(last, dict) and last.get("p"):
            return last["p"]
        for section in ("min", "day", "prevDay"):
            bar = t.get(section)
            if isinstance(bar, dict) and bar.get("c"):
                return bar["c"]
        return None

    # Previous-close aggregate shape: {"results": [{"c": ..}]}
    if data.get("status") not in (None, "OK") or data.get("resultsCount") == 0:
        return None
    results = data.get("results", data)
    if isinstance(results, list):
        results = results[0] if results else None
    if isinstance(results, dict):
        for key in ("c", "p", "price", "last_price", "vw"):
            if key in results:
                return results[key]
    return None


def _extract_latest_price(data: dict) -> float | None:
    """Pull the trade price out of the Massive 'previous close' response shape.

    The /v2/aggs/ticker/{symbol}/prev endpoint returns "results" as a LIST
    containing a single aggregate bar (not a dict), e.g.:
        {"status": "OK", "resultsCount": 1, "results": [{"c": 148.845, ...}]}
    Previously this code treated "results" as a dict, so isinstance(results, dict)
    was always False for this endpoint's real shape and the price silently
    resolved to None. Unwrap the list here, and check "status"/"resultsCount"
    so invalid tickers (empty results) are detected instead of "succeeding"
    with a null price.

    Adjust the key lookup here if the real Massive API returns a different
    field name for the traded/close price.
    """
    if not isinstance(data, dict):
        return None
    if data.get("status") not in (None, "OK") or data.get("resultsCount") == 0:
        return None
    results = data.get("results", data)
    if isinstance(results, list):
        results = results[0] if results else None
    if isinstance(results, dict):
        for key in ("c", "p", "price", "last_price", "vw"):
            if key in results:
                return results[key]
    return None




def _upsert_batch(items: list[dict]) -> int:
    """Upsert a batch of Massive API items into Lakebase, one statement per row.

    For very large batches, consider psycopg2.extras.execute_values for
    higher throughput instead of per-row execute calls.
    """
    import json as _json

    count = 0
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            for item in items:
                cur.execute(
                    f"""
                    INSERT INTO {TABLE_NAME} (id, payload, synced_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (id) DO UPDATE
                        SET payload = EXCLUDED.payload,
                            synced_at = EXCLUDED.synced_at
                    """,
                    (str(item.get("id")), _json.dumps(item)),
                )
                count += 1
            conn.commit()
    return count


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    app.run(debug=True, host=host, port=port)
    print(f"Flask app running on http://{host}:{port}")