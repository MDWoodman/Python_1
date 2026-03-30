from datetime import datetime
import sqlite3

from flask import Flask, jsonify, request

from config import conf as cnf


app = Flask(__name__)


def _get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(cnf.DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_signals_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT,
            trade TEXT,
            symbol TEXT,
            status TEXT
        )
        """
    )


def _normalize_trade(trade: str | None) -> str | None:
    if trade is None:
        return None
    normalized = trade.strip().upper()
    if normalized in {"BUY", "SELL"}:
        return normalized
    return None


def _latest_signal_status(conn: sqlite3.Connection, symbol: str, trade: str, statuses: tuple[str, ...]) -> dict | None:
    placeholders = ",".join(["?"] * len(statuses))
    sql = (
        f"SELECT id, time, trade, symbol, status "
        f"FROM signals "
        f"WHERE symbol = ? AND trade = ? AND status IN ({placeholders}) "
        f"ORDER BY id DESC LIMIT 1"
    )
    params = [symbol, trade, *statuses]
    row = conn.execute(sql, params).fetchone()
    if row is None:
        return None

    return {
        "id": row["id"],
        "time": row["time"],
        "trade": row["trade"],
        "symbol": row["symbol"],
        "status": row["status"],
    }


def _transaction_state(conn: sqlite3.Connection, symbol: str, trade: str) -> tuple[bool, bool]:
    table_name = f"transactions_{cnf.PERIOD}"

    try:
        open_row = conn.execute(
            f"SELECT 1 FROM {table_name} WHERE symbol = ? AND buy_sell = ? AND open_close = 'OPEN' ORDER BY id DESC LIMIT 1",
            (symbol, trade),
        ).fetchone()
        close_row = conn.execute(
            f"SELECT 1 FROM {table_name} WHERE symbol = ? AND buy_sell = ? AND open_close = 'CLOSE' ORDER BY id DESC LIMIT 1",
            (symbol, trade),
        ).fetchone()
    except sqlite3.OperationalError:
        return False, False

    return open_row is not None, close_row is not None


def _build_notification(conn: sqlite3.Connection, symbol: str, trade: str) -> dict:
    open_signal = _latest_signal_status(conn, symbol, trade, ("TO OPEN", "OPEN TRANSACTION", "OPENED"))
    close_signal = _latest_signal_status(conn, symbol, trade, ("TO CLOSE", "CLOSED"))
    is_open, is_closed = _transaction_state(conn, symbol, trade)

    return {
        "symbol": symbol,
        "trade": trade,
        "open_signal": open_signal,
        "is_open": is_open,
        "close_signal": close_signal,
        "is_closed": is_closed,
    }


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET,OPTIONS"
    return response


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat() + "Z"})


@app.route("/api/notifications", methods=["GET"])
def notifications():
    symbol_filter = request.args.get("symbol", type=str)
    trade_filter = _normalize_trade(request.args.get("trade", type=str))

    with _get_connection() as conn:
        _ensure_signals_table(conn)

        if symbol_filter:
            symbols = [symbol_filter.strip().upper()]
        else:
            symbols = list(cnf.SYMBOLS_LIST)

        trades = [trade_filter] if trade_filter else ["BUY", "SELL"]

        items = []
        for symbol in symbols:
            for trade in trades:
                items.append(_build_notification(conn, symbol, trade))

    return jsonify(
        {
            "count": len(items),
            "period": cnf.PERIOD,
            "notifications": items,
        }
    )


@app.route("/api/notifications/open", methods=["GET"])
def notifications_open():
    payload = notifications().get_json()
    filtered = [item for item in payload["notifications"] if item["open_signal"] is not None]
    payload["count"] = len(filtered)
    payload["notifications"] = filtered
    return jsonify(payload)


@app.route("/api/notifications/close", methods=["GET"])
def notifications_close():
    payload = notifications().get_json()
    filtered = [item for item in payload["notifications"] if item["close_signal"] is not None]
    payload["count"] = len(filtered)
    payload["notifications"] = filtered
    return jsonify(payload)


if __name__ == "__main__":
    host = cnf.API_HOST or "0.0.0.0"
    port = int(cnf.API_PORT)
    app.run(debug=False, host=host, port=port)
