from __future__ import annotations

from database import database as db
import pandas as pd
import tools
from wskazniki import mcad__chat as mcd


def _print_mcad_terminal_report(result: dict) -> None:
    def _fmt(value: object) -> str:
        if value is None:
            return "BRAK"
        if isinstance(value, float) and pd.isna(value):
            return "BRAK"
        if isinstance(value, (int, float)):
            return f"{float(value):.6f}"
        return str(value)

    print("\n" + "=" * 72)
    print("MCAD REPORT")
    print("=" * 72)
    print(
        f"Instrument: {result['symbol']}  |  Period: {result['period']} (db={result['db_period']})"
    )
    print(
        f"Candles: {result['candles_count']}  |  Closed only: {result['closed_candles_only']}"
    )
    print("-" * 72)
    print(
        f"Last candle: {result['last_candle_time_text']}  |  MACD: {_fmt(result['last_macd'])}  "
        f"|  Signal: {_fmt(result['last_signal_line'])}  |  Hist: {_fmt(result['last_histogram'])}"
    )
    print("-" * 72)
    print(
        f"Result: {result['mcad_result']}  |  Trend: {result['trend']}  |  Cross: {result['raw_cross'] or 'BRAK'}"
    )
    print(
        f"Result time: {result['time_result_text']}  |  Cross time: {result['raw_cross_time_text'] or 'BRAK'}"
    )
    print("-" * 72)
    print("Signal rules:")
    print("BUY  when MACD crosses above Signal Line (cross=WZROST)")
    print("SELL when MACD crosses below Signal Line (cross=SPADEK)")
    print("-" * 72)
    print(f"BUY: {result['buy_signal']}  |  SELL: {result['sell_signal']}")
    decision = "BUY" if result["buy_signal"] else ("SELL" if result["sell_signal"] else "BRAK")
    print(f"Active decision now: {decision}")
    print("=" * 72)


def _normalize_db_period(period: str) -> str:
    mapping = {
        "M1": "1",
        "M5": "5",
        "M15": "15",
        "M30": "30",
        "H1": "60",
        "H4": "240",
        "D1": "1440",
    }
    period_upper = str(period).upper()
    return mapping.get(period_upper, str(period))


def test_mcad_on_database_data(
    symbol: str = "UK100",
    period: str = "M5",
    candles_count: int = 300,
    short_window: int = 12,
    long_window: int = 26,
    signal_window: int = 9,
    angle: float = 45,
    closed_candles_only: bool = True,
) -> dict:
    db_period = _normalize_db_period(period)
    raw_rows = db.get_last_candle_from_database(symbol, db_period, candles_count)
    if not raw_rows:
        raise ValueError(
            f"Brak danych świec w bazie dla {symbol}/{period} (tabela candles_{symbol}_{db_period})."
        )

    # DB returns newest first, so reverse for indicator calculations.
    ordered_rows = list(reversed(raw_rows))

    if closed_candles_only:
        if len(ordered_rows) < 2:
            raise ValueError("Za mało świec, aby pominąć ostatnią (otwartą) świecę")
        ordered_rows = ordered_rows[:-1]

    mcad_obj = mcd.mcad_object()
    mcad_df = mcad_obj.get_data_from_candle_array(ordered_rows)

    mcad_result, time_result, trend, raw_cross, raw_cross_time = mcad_obj.analyze_mcad(
        mcad_df,
        short_window=short_window,
        long_window=long_window,
        signal_window=signal_window,
        angle=angle,
    )

    if time_result is None:
        raise ValueError("analyze_mcad nie zwrócił czasu wyniku")

    calc_df = mcad_obj.calculate_mcad(
        mcad_df,
        short_window=short_window,
        long_window=long_window,
        signal_window=signal_window,
    )
    last = calc_df.iloc[-1]

 
    result = {
        "symbol": symbol,
        "period": period,
        "db_period": db_period,
        "candles_count": len(mcad_df),
        "closed_candles_only": closed_candles_only,
        "last_candle_time": int(last["Date"]),
        "last_candle_time_text": tools.int_to_datetime(int(last["Date"])),
        "last_macd": float(last["MACD"]) if last["MACD"] is not None else None,
        "last_signal_line": float(last["Signal Line"]) if last["Signal Line"] is not None else None,
        "last_histogram": float(last["MACD_histogram"]) if last["MACD_histogram"] is not None else None,
        "mcad_result": str(mcad_result),
        "trend": str(trend),
        "time_result": time_result,
        "time_result_text": tools.int_to_datetime(time_result),
        "raw_cross": raw_cross,
        "raw_cross_time": raw_cross_time,
        "raw_cross_time_text": tools.int_to_datetime(raw_cross_time) if raw_cross_time is not None else None,
        "buy_signal": mcad_result == mcd.mcad_result_enum.Wzrost_przeciecie,
        "sell_signal": mcad_result == mcd.mcad_result_enum.Spadek_przeciecie,
    }

    _print_mcad_terminal_report(result)

    return result


def print_mcad_table_from_database(
    symbol: str = "UK100",
    period: str = "M5",
    candles_count: int = 300,
    short_window: int = 12,
    long_window: int = 26,
    signal_window: int = 9,
    closed_candles_only: bool = True,
) -> None:
    db_period = _normalize_db_period(period)
    raw_rows = db.get_last_candle_from_database(symbol, db_period, candles_count)
    if not raw_rows:
        raise ValueError(
            f"Brak danych świec w bazie dla {symbol}/{period} (tabela candles_{symbol}_{db_period})."
        )

    ordered_rows = list(reversed(raw_rows))
    if closed_candles_only:
        if len(ordered_rows) < 2:
            raise ValueError("Za mało świec, aby pominąć ostatnią (otwartą) świecę")
        ordered_rows = ordered_rows[:-1]

    mcad_obj = mcd.mcad_object()
    data = mcad_obj.get_data_from_candle_array(ordered_rows)
    calc_df = mcad_obj.calculate_mcad(
        data,
        short_window=short_window,
        long_window=long_window,
        signal_window=signal_window,
    )

    print("=== MCAD DLA KAZDEJ SWIECY (DB) ===")
    print(
        f"symbol={symbol} period={period} db_period={db_period} candles={len(calc_df)} "
        f"closed_candles_only={closed_candles_only}"
    )
    print("time | close | macd | signal | histogram | cross | buy_sell")

    for idx in range(len(calc_df)):
        row = calc_df.iloc[idx]

        row_time = int(row["Date"])
        close_value = float(row["Close"]) if pd.notna(row["Close"]) else None
        macd_value = float(row["MACD"]) if pd.notna(row["MACD"]) else None
        signal_value = float(row["Signal Line"]) if pd.notna(row["Signal Line"]) else None
        histogram_value = float(row["MACD_histogram"]) if pd.notna(row["MACD_histogram"]) else None

        cross = "BRAK"
        buy_sell = "BRAK"
        if idx > 0:
            prev = calc_df.iloc[idx - 1]
            prev_macd = prev.get("MACD")
            prev_signal = prev.get("Signal Line")
            curr_macd = row.get("MACD")
            curr_signal = row.get("Signal Line")

            if all(pd.notna(value) for value in [prev_macd, prev_signal, curr_macd, curr_signal]):
                if prev_macd <= prev_signal and curr_macd > curr_signal:
                    cross = "WZROST"
                    buy_sell = "BUY"
                elif prev_macd >= prev_signal and curr_macd < curr_signal:
                    cross = "SPADEK"
                    buy_sell = "SELL"

        print(
            f"{tools.int_to_datetime(row_time)} | "
            f"{close_value:.2f} | "
            f"{macd_value:.6f} | "
            f"{signal_value:.6f} | "
            f"{histogram_value:.6f} | "
            f"{cross} | "
            f"{buy_sell}"
        )


if __name__ == "__main__":
    print_mcad_table_from_database(symbol="UK100", period="M5", candles_count=300)
    test_mcad_on_database_data(symbol="UK100", period="M5", candles_count=300)
