from datetime import datetime
from pathlib import Path
import logging as logger

import tools as tools


def _format_broker_time(broker_time_ms: int | None) -> str:
    if broker_time_ms is None:
        return "BRAK"

    try:
        return tools.int_to_datetime(int(broker_time_ms)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "BRAK"


def log_trade_audit_event(
    symbol: str,
    event_type: str,
    signal: str,
    broker_time_ms: int | None,
    open_scenario_number: int | None = None,
    close_scenario_number: int | None = None,
    scenario_conditions: str | None = None,
) -> None:
    try:
        broker_time_text = _format_broker_time(broker_time_ms)
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        open_sc = f"SC{open_scenario_number}" if open_scenario_number is not None else "BRAK"
        close_sc = f"C{close_scenario_number}" if close_scenario_number is not None else "BRAK"

        message = (
            f"AUDYT_SYGNALU | produkt={symbol} | event={event_type} | sygnal={signal} | "
            f"godzina_brokera={broker_time_text} | scenariusz_otwarcia={open_sc} | "
            f"scenariusz_zamkniecia={close_sc} | data_godzina_lokalna={now_text} | "
            f"warunki={scenario_conditions or 'BRAK'}"
        )

        logger.info(message)

        logs_dir = Path(__file__).resolve().parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        file_path = logs_dir / f"audyt_sygnalow_{symbol}.txt"
        with file_path.open("a", encoding="utf-8") as file:
            file.write(message + "\n")
    except Exception as log_error:
        logger.error(f"Nie udalo sie zapisac logu audytu sygnalu dla {symbol}: {log_error}")
