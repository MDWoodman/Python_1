from __future__ import annotations

import csv
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config import conf as cnf


PROJECT_ROOT = Path(__file__).resolve().parent
LOGS_DIR = PROJECT_ROOT / "logs"
RESULTS_DIR = LOGS_DIR / "wyniki_sygnalow"


class OpenAction(BaseModel):
    should_open: bool
    side: str | None
    scenario_number: int | None
    timestamp: str | None
    source: str
    details: str | None


class PositionStatus(BaseModel):
    is_open: bool
    side: str | None
    status: str
    timestamp: str | None
    scenario_number: int | None


class CloseAction(BaseModel):
    should_close: bool
    side: str | None
    scenario_number: int | None
    timestamp: str | None
    source: str
    details: str | None


class SymbolDecisionResponse(BaseModel):
    symbol: str
    period: str
    open_action: OpenAction
    position_status: PositionStatus
    close_action: CloseAction


app = FastAPI(
    title="Trading Scenario API",
    version="1.0.0",
    description="API for mobile/web clients (Kotlin, .NET Core, React) with open/close scenario decisions per symbol.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)


def _db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(cnf.DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_scenario_number(value: Any) -> int | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.upper() == "BRAK":
        return None

    match = re.search(r"(\d+)", text)
    if not match:
        return None

    try:
        return int(match.group(1))
    except ValueError:
        return None


def _read_latest_result_row(symbol: str) -> dict[str, str] | None:
    file_path = RESULTS_DIR / f"{symbol}.csv"
    if not file_path.exists():
        return None

    latest: dict[str, str] | None = None
    with file_path.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            latest = row

    return latest


def _open_action_from_logs(symbol: str) -> OpenAction:
    latest = _read_latest_result_row(symbol)
    if latest is None:
        return OpenAction(
            should_open=False,
            side=None,
            scenario_number=None,
            timestamp=None,
            source="logs/wyniki_sygnalow",
            details="Brak danych sygnalow dla symbolu",
        )

    signal = (latest.get("sygnal") or "BRAK").strip().upper()
    side = signal if signal in {"BUY", "SELL"} else None

    date_text = (latest.get("data") or "").strip()
    time_text = (latest.get("godzina") or "").strip()
    timestamp = f"{date_text} {time_text}".strip() if date_text or time_text else None

    return OpenAction(
        should_open=side is not None,
        side=side,
        scenario_number=_parse_scenario_number(latest.get("nr_scenariusza")),
        timestamp=timestamp,
        source="logs/wyniki_sygnalow",
        details=latest.get("warunki_scenariusza"),
    )


def _latest_transaction(symbol: str, period: str) -> sqlite3.Row | None:
    table_name = f"transactions_{period}"
    try:
        with _db_connection() as conn:
            row = conn.execute(
                f"SELECT id, symbol, time, buy_sell, open_close FROM {table_name} WHERE symbol = ? ORDER BY id DESC LIMIT 1",
                (symbol,),
            ).fetchone()
        return row
    except sqlite3.OperationalError:
        return None


def _position_status(symbol: str, period: str, open_action: OpenAction) -> PositionStatus:
    tx = _latest_transaction(symbol, period)
    if tx is None:
        return PositionStatus(
            is_open=False,
            side=None,
            status="UNKNOWN",
            timestamp=None,
            scenario_number=open_action.scenario_number,
        )

    status = str(tx["open_close"]).upper()
    side = str(tx["buy_sell"]).upper() if tx["buy_sell"] is not None else None

    return PositionStatus(
        is_open=status == "OPEN",
        side=side,
        status=status,
        timestamp=str(tx["time"]) if tx["time"] is not None else None,
        scenario_number=open_action.scenario_number,
    )


def _read_last_line(file_path: Path) -> str | None:
    if not file_path.exists():
        return None

    last: str | None = None
    with file_path.open("r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                last = line.strip()
    return last


def _latest_close_signal_from_db(symbol: str) -> tuple[str | None, str | None]:
    try:
        with _db_connection() as conn:
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
            row = conn.execute(
                "SELECT time, trade FROM signals WHERE symbol = ? AND status = 'TO CLOSE' ORDER BY id DESC LIMIT 1",
                (symbol,),
            ).fetchone()
        if row is None:
            return None, None
        return str(row["time"]), str(row["trade"]).upper() if row["trade"] is not None else None
    except Exception:
        return None, None


def _close_action_from_logs(symbol: str) -> CloseAction:
    file_path = LOGS_DIR / f"zakoncz_scenariusz_{symbol}.txt"
    last_line = _read_last_line(file_path)
    if not last_line:
        close_time, close_side = _latest_close_signal_from_db(symbol)
        if close_side in {"BUY", "SELL"}:
            return CloseAction(
                should_close=True,
                side=close_side,
                scenario_number=None,
                timestamp=close_time,
                source="database/signals",
                details="Signal TO CLOSE without scenario number",
            )
        return CloseAction(
            should_close=False,
            side=None,
            scenario_number=None,
            timestamp=None,
            source=f"logs/zakoncz_scenariusz_{symbol}.txt",
            details="Brak sygnalu zamkniecia",
        )

    # Example:
    # ZAMKNIECIE SCENARIUSZ | symbol=UK100 | data_godzina=2026-03-07 10:15:00 | typ=BUY | numer_scenariusza=C1
    match = re.search(
        r"data_godzina=([^|]+)\s*\|\s*typ=(BUY|SELL)\s*\|\s*numer_scenariusza=C(\d+)",
        last_line,
    )

    if not match:
        close_time, close_side = _latest_close_signal_from_db(symbol)
        if close_side in {"BUY", "SELL"}:
            return CloseAction(
                should_close=True,
                side=close_side,
                scenario_number=None,
                timestamp=close_time,
                source="database/signals",
                details=last_line,
            )
        return CloseAction(
            should_close=False,
            side=None,
            scenario_number=None,
            timestamp=None,
            source=f"logs/zakoncz_scenariusz_{symbol}.txt",
            details=last_line,
        )

    return CloseAction(
        should_close=True,
        side=match.group(2),
        scenario_number=int(match.group(3)),
        timestamp=match.group(1).strip(),
        source=f"logs/zakoncz_scenariusz_{symbol}.txt",
        details=last_line,
    )


def _symbol_decision(symbol: str, period: str) -> SymbolDecisionResponse:
    normalized_symbol = symbol.strip().upper()
    normalized_period = (period or cnf.PERIOD).upper()

    open_action = _open_action_from_logs(normalized_symbol)
    position_status = _position_status(normalized_symbol, normalized_period, open_action)
    close_action = _close_action_from_logs(normalized_symbol)

    return SymbolDecisionResponse(
        symbol=normalized_symbol,
        period=normalized_period,
        open_action=open_action,
        position_status=position_status,
        close_action=close_action,
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "time": datetime.utcnow().isoformat() + "Z",
    }


@app.get("/api/v1/symbols/{symbol}", response_model=SymbolDecisionResponse)
def get_symbol_decision(
    symbol: str,
    period: str = Query(default=cnf.PERIOD, description="Trading period, e.g. H1/H4/M5"),
) -> SymbolDecisionResponse:
    return _symbol_decision(symbol, period)


@app.get("/api/v1/symbols", response_model=list[SymbolDecisionResponse])
def get_symbols_decisions(
    period: str = Query(default=cnf.PERIOD, description="Trading period, e.g. H1/H4/M5"),
) -> list[SymbolDecisionResponse]:
    return [_symbol_decision(symbol, period) for symbol in cnf.SYMBOLS_LIST]


if __name__ == "__main__":
    import uvicorn

    host = cnf.API_HOST or "0.0.0.0"
    try:
        port = int(cnf.API_PORT)
    except Exception:
        port = 8000

    uvicorn.run("API:app", host=host, port=port, reload=False)
