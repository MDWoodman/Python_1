from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from wskazniki import adx__chat as adxcht
import tools


RESULTS_DIR = Path(__file__).resolve().parent / "logs" / "wyniki_sygnalow"

CSV_FIELDS = [
    "data",
    "godzina",
    "godzina_brokera",
    "typ_zdarzenia",
    "rezultat_adx",
    "adx_di_cross_raw",
    "rezultat_mcad",
    "rezultat_ichimoku",
    "sygnal",
    "nr_scenariusza",
    "nr_scenariusza_zamkniecia",
    "warunki_scenariusza",
]


def _to_text(value) -> str:
    if value is None:
        return "BRAK"
    if hasattr(value, "name"):
        return str(value.name)
    return str(value)


def _adx_text(adx_analyze_result_obj) -> str:
    if adx_analyze_result_obj is None or adx_analyze_result_obj[0] is None:
        return "BRAK"

    adx_result = _to_text(adx_analyze_result_obj[0].get_result())
    adx_trend = _to_text(adx_analyze_result_obj[1])
    adx_time = _to_text(adx_analyze_result_obj[0].get_time())
    return f"{adx_result} ({adx_trend}) {adx_time}"


def _adx_raw_di_cross_text(adx_analyze_result_obj) -> str:
    if adx_analyze_result_obj is None or adx_analyze_result_obj[0] is None:
        return "BRAK"

    adx_obj = adx_analyze_result_obj[0]
    cross = _to_text(adx_obj.get_raw_di_cross())
    cross_time = _to_text(adx_obj.get_raw_di_cross_time())

    if cross == "BRAK":
        return "BRAK"

    return f"{cross} {cross_time}"


def _mcad_text(mcad_analyze_result_obj) -> str:
    if mcad_analyze_result_obj is None:
        return "BRAK"
    return _to_text(mcad_analyze_result_obj.get_result())


def _extract_candle_datetime(adx_analyze_result_obj, mcad_analyze_result_obj) -> datetime | None:
    adx_time = None
    if adx_analyze_result_obj is not None and adx_analyze_result_obj[0] is not None:
        adx_time = adx_analyze_result_obj[0].get_time()

    if adx_time:
        return tools.int_to_datetime(adx_time)

    if mcad_analyze_result_obj is not None:
        mcad_time = mcad_analyze_result_obj.get_time()
        if mcad_time:
            return tools.int_to_datetime(mcad_time)

    return None
    

def _ichimoku_text(ichimoku_result_K: list[str], ichimoku_result_S: list[str]) -> str:
    if not ichimoku_result_K and not ichimoku_result_S:
        return "BRAK"

    result_k = " | ".join(ichimoku_result_K) if ichimoku_result_K else "BRAK"
    result_s = " | ".join(ichimoku_result_S) if ichimoku_result_S else "BRAK"
    return f"K: {result_k} ; S: {result_s}"


def _ensure_csv_header(file_path: Path) -> None:
    if not file_path.exists():
        return

    with file_path.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        current_fields = reader.fieldnames or []
        if current_fields == CSV_FIELDS:
            return
        rows = list(reader)

    with file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            normalized_row = {field: row.get(field, "BRAK") for field in CSV_FIELDS}
            writer.writerow(normalized_row)


def log_symbol_result(
    symbol: str,
    adx_analyze_result_obj,
    mcad_analyze_result_obj,
    ichimoku_result_K: list[str],
    ichimoku_result_S: list[str],
    signal: str,
    scenario_number: int | None = None,
    close_scenario_number: int | None = None,
    scenario_conditions: str | None = None,
    broker_time_ms: int | None = None,
    event_type: str | None = None,
) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    file_path = RESULTS_DIR / f"{symbol}.csv"
    _ensure_csv_header(file_path)

    candle_datetime = _extract_candle_datetime(adx_analyze_result_obj, mcad_analyze_result_obj)
    now = candle_datetime or datetime.now()
    row = {
        "data": now.strftime("%Y-%m-%d"),
        "godzina": now.strftime("%H:%M:%S"),
        "godzina_brokera": tools.int_to_datetime(broker_time_ms).strftime("%Y-%m-%d %H:%M:%S") if broker_time_ms else "BRAK",
        "typ_zdarzenia": event_type or "ANALIZA",
        "rezultat_adx": _adx_text(adx_analyze_result_obj),
        "adx_di_cross_raw": _adx_raw_di_cross_text(adx_analyze_result_obj),
        "rezultat_mcad": _mcad_text(mcad_analyze_result_obj),
        "rezultat_ichimoku": _ichimoku_text(ichimoku_result_K, ichimoku_result_S),
        "sygnal": signal or "BRAK",
        "nr_scenariusza": scenario_number if scenario_number is not None else "BRAK",
        "nr_scenariusza_zamkniecia": close_scenario_number if close_scenario_number is not None else "BRAK",
        "warunki_scenariusza": scenario_conditions or "BRAK",
    }

    write_header = not file_path.exists()
    with file_path.open("a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

    return file_path
