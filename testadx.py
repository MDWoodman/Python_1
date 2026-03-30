from __future__ import annotations

import pandas as pd

import tools
from database import database as db
from wskazniki import adx__chat as adx


def _rows_to_dataframe(raw_rows: list[tuple]) -> pd.DataFrame:
    ordered_rows = list(reversed(raw_rows))
    return pd.DataFrame(
        {
            "Date": [row[0] for row in ordered_rows],
            "Open": [row[1] for row in ordered_rows],
            "High": [row[2] for row in ordered_rows],
            "Low": [row[3] for row in ordered_rows],
            "Close": [row[4] for row in ordered_rows],
            "Volume": [row[5] for row in ordered_rows],
        }
    )


def _detect_raw_di_cross(adx_df: pd.DataFrame) -> tuple[str, int | None]:
    if len(adx_df) < 2:
        return "BRAK", None

    prev = adx_df.iloc[-2]
    last = adx_df.iloc[-1]

    prev_plus = prev.get("+DI")
    prev_minus = prev.get("-DI")
    last_plus = last.get("+DI")
    last_minus = last.get("-DI")

    if any(pd.isna(value) for value in [prev_plus, prev_minus, last_plus, last_minus]):
        return "BRAK", None

    if prev_plus <= prev_minus and last_plus > last_minus:
        return "WZROST", int(adx_df.iloc[-1]["Date"])

    if prev_plus >= prev_minus and last_plus < last_minus:
        return "SPADEK", int(adx_df.iloc[-1]["Date"])

    return "BRAK", None


def _adx_trend(prev_adx: float | None, current_adx: float | None) -> str:
    if prev_adx is None or current_adx is None:
        return "BRAK"
    if current_adx > prev_adx:
        return "INCREASING"
    if current_adx < prev_adx:
        return "DECREASING"
    return "NEITHER"


def _raw_cross_for_row(prev_row: pd.Series, row: pd.Series) -> tuple[str, int | None]:
    prev_plus = prev_row.get("+DI")
    prev_minus = prev_row.get("-DI")
    curr_plus = row.get("+DI")
    curr_minus = row.get("-DI")

    if any(pd.isna(value) for value in [prev_plus, prev_minus, curr_plus, curr_minus]):
        return "BRAK", None

    candle_time = int(row["Date"])
    if prev_plus <= prev_minus and curr_plus > curr_minus:
        return "WZROST", candle_time
    if prev_plus >= prev_minus and curr_plus < curr_minus:
        return "SPADEK", candle_time
    return "BRAK", None


def _trade_signal_from_cross(cross: str) -> str:
    if cross == "WZROST":
        return "BUY"
    if cross == "SPADEK":
        return "SELL"
    return "BRAK"


def build_adx_table_from_database(
    symbol: str = "US30",
    period: str = "M30",
    candles_count: int = 250,
    adx_window: int = 14,
    adx_threshold: float = 30,
    closed_candles_only: bool = True,
) -> pd.DataFrame:
    db_period = _normalize_db_period(period)
    raw_rows = db.get_last_candle_from_database(symbol, db_period, candles_count)
    if not raw_rows:
        raise ValueError(
            f"Brak danych świec w bazie dla {symbol}/{period} (tabela candles_{symbol}_{db_period}). "
            "Uruchom service.py na M5, aby pobrać i zapisać świece do bazy."
        )

    adx_obj = adx.adx_object()
    data = _rows_to_dataframe(raw_rows)
    if closed_candles_only:
        if len(data) < 2:
            raise ValueError("Za mało świec, aby pominąć ostatnią (otwartą) świecę")
        data = data.iloc[:-1].copy()

    adx_df = adx_obj.calculate_adx(data, period=adx_window)
    if adx_df.empty:
        raise ValueError("Obliczenia ADX zwróciły pusty DataFrame")


    print("=== ADX DataFrame ===")
    print(adx_df.to_string())
    print()

    table_rows: list[dict] = []

    for idx in range(len(adx_df)):
        row = adx_df.iloc[idx]
        row_time = int(row["Date"])
        prev_row = adx_df.iloc[idx - 1] if idx > 0 else None

        current_adx = float(row["ADX"]) if pd.notna(row["ADX"]) else None
        prev_adx = None
        if prev_row is not None and pd.notna(prev_row["ADX"]):
            prev_adx = float(prev_row["ADX"])

        trend_value = _adx_trend(prev_adx, current_adx)

        raw_cross, raw_cross_time = ("BRAK", None)
        state = "Boczny"
        if prev_row is not None:
            raw_cross, raw_cross_time = _raw_cross_for_row(prev_row, row)
           
            if raw_cross == "WZROST":
                state = "Wzrost_przeciecie"
            elif raw_cross == "SPADEK":
                state = "Spadek_przeciecie"

        table_rows.append(
            {
                "data godzina": tools.int_to_datetime(row_time),
                "stan adx": state,
                "trend adx": trend_value,
                "sygnal": _trade_signal_from_cross(raw_cross),
                "data godzina przeciecia adx": tools.int_to_datetime(raw_cross_time) if raw_cross_time is not None else "BRAK",
                }
            )

    result_df = pd.DataFrame(table_rows)

    print("=== TABELA ADX DLA KAŻDEJ ŚWIECY ===")
    print(result_df.to_string(index=False))

    return result_df


def _normalize_db_period(period: str) -> str:
    mapping = {
        "M1": "1",
        "M5": "5",
        "M15": "15",
        "M30": "30",
        "H1": "60",
        "H4": "240",
    }
    period_upper = str(period).upper()
    return mapping.get(period_upper, str(period))


def test_adx_on_database_data(
    symbol: str = "US30",
    period: str = "M30",
    candles_count: int = 250,
    adx_window: int = 14,
    adx_threshold: float = 30,
    closed_candles_only: bool = True,
) -> dict:
    db_period = _normalize_db_period(period)
    raw_rows = db.get_last_candle_from_database(symbol, db_period, candles_count)
    if not raw_rows:
        raise ValueError(
            f"Brak danych świec w bazie dla {symbol}/{period} (tabela candles_{symbol}_{db_period}). "
            "Uruchom service.py na M5, aby pobrać i zapisać świece do bazy."
        )

    adx_obj = adx.adx_object()
    data = _rows_to_dataframe(raw_rows)
    if closed_candles_only:
        if len(data) < 2:
            raise ValueError("Za mało świec, aby pominąć ostatnią (otwartą) świecę")
        data = data.iloc[:-1].copy()

    adx_df = adx_obj.calculate_adx(data, period=adx_window)

    if adx_df.empty:
        raise ValueError("Obliczenia ADX zwróciły pusty DataFrame")

    last = adx_df.iloc[-1]

    adx_analyze_result_obj, adx_trend = adx.analyze_adx_candles(
        data,
        adx_window,
        adx_threshold,
        symbol,
        period,
    )

    if adx_analyze_result_obj is None or adx_trend is None:
        raise ValueError("analyze_adx_candles zwróciło pusty wynik (None)")

    adx_result = adx_analyze_result_obj.get_result()
    raw_cross = adx_analyze_result_obj.get_raw_di_cross() or "BRAK"
    raw_cross_time = adx_analyze_result_obj.get_raw_di_cross_time()

    print("=== DI+ i DI- PRZECIĘCIA ===")
    for idx in range(len(adx_df)):
        row = adx_df.iloc[idx]
        prev_row = adx_df.iloc[idx - 1] if idx > 0 else None
        
        if prev_row is not None:
            cross_type, cross_time = _raw_cross_for_row(prev_row, row)
            if cross_type != "BRAK":
                print(f"Czas: {tools.int_to_datetime(cross_time)} | Typ: {cross_type} | +DI: {float(row['+DI']):.2f} | -DI: {float(row['-DI']):.2f}")
    
    print("\n=== TREND ADX ===")
    print(f"Trend: {adx_trend}")
    result = {
        "symbol": symbol,
        "period": period,
        "db_period": db_period,
        "candles_count": len(data),
        "closed_candles_only": closed_candles_only,
        "last_candle_time": int(last["Date"]),
        "last_candle_time_text": tools.int_to_datetime(int(last["Date"])),
        "last_plus_di": float(last["+DI"]) if pd.notna(last["+DI"]) else None,
        "last_minus_di": float(last["-DI"]) if pd.notna(last["-DI"]) else None,
        "last_adx": float(last["ADX"]) if pd.notna(last["ADX"]) else None,
        "adx_result": str(adx_result),
        "signal": "BUY" if adx_result == adx.adx_result_enum.Wzrost_przeciecie else ("SELL" if adx_result == adx.adx_result_enum.Spadek_przeciecie else "BRAK"),
        "adx_trend": str(adx_trend),
        "analyze_result_time": adx_analyze_result_obj.get_time(),
        "analyze_result_time_text": tools.int_to_datetime(adx_analyze_result_obj.get_time()),
        "buy_signal": adx_result == adx.adx_result_enum.Wzrost_przeciecie,
        "sell_signal": adx_result == adx.adx_result_enum.Spadek_przeciecie,
        "raw_di_cross": raw_cross,
        "raw_di_cross_time": raw_cross_time,
        "raw_di_cross_time_text": tools.int_to_datetime(raw_cross_time) if raw_cross_time is not None else None,
    }

    print("=== TEST ADX (DB) ===")
    for key, value in result.items():
        print(f"{key}: {value}")

    return result


if __name__ == "__main__":
    test_adx_on_database_data(symbol="Uk100", period="M5", candles_count=300, adx_window=14, adx_threshold=30)
