from __future__ import annotations

import pandas as pd

import tools
from database import database as db
from wskazniki import ichi__chat as ichi


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


def _prepare_candles(symbol: str, period: str, candles_count: int, closed_candles_only: bool) -> list[tuple]:
    db_period = _normalize_db_period(period)
    raw_rows = db.get_last_candle_from_database(symbol, db_period, candles_count)
    if not raw_rows:
        raise ValueError(f"Brak danych swiec w bazie dla {symbol}/{period} (tabela candles_{symbol}_{db_period}).")

    # DB returns newest first, indicator calculations need oldest first.
    ordered_rows = list(reversed(raw_rows))

    if closed_candles_only:
        if len(ordered_rows) < 2:
            raise ValueError("Za malo swiec, aby pominac ostatnia (otwarta) swiece")
        ordered_rows = ordered_rows[:-1]

    return ordered_rows


def test_ichi_on_database_data(
    symbol: str = "UK100",
    period: str = "M5",
    candles_count: int = 300,
    tenkansen_period: int = 9,
    kiusen_period: int = 26,
    senokuspanB_period: int = 52,
    last_n_candles: int = 30,
    closed_candles_only: bool = True,
) -> dict:
    ordered_rows = _prepare_candles(symbol, period, candles_count, closed_candles_only)
    db_period = _normalize_db_period(period)

    ichi_obj = ichi.ichimoku_object()
    data_df = ichi_obj.get_data_from_candle_array(ordered_rows)
    tail_df = data_df.tail(int(last_n_candles)).copy()

    ichi_result = ichi_obj.analyze_ichimoku(
        data_df,
        tail_df,
        tenkansen_period=tenkansen_period,
        kiusen_period=kiusen_period,
        senokuspanB_period=senokuspanB_period,
    )

    calc_df = ichi_obj.calculate_ichimoku(
        data_df,
        tenkansen=tenkansen_period,
        kiusen=kiusen_period,
        senokuspan=senokuspanB_period,
    )

    analyze_obj, trend = ichi.analyze_ichimoku_candles(
        data_df,
        tail_df,
        tenkansen_period=tenkansen_period,
        kiusen_period=kiusen_period,
        senokuspanB_period=senokuspanB_period,
        symbol=symbol,
        period=period,
    )

    if analyze_obj is None or trend is None:
        raise ValueError("analyze_ichimoku_candles zwrocilo pusty wynik (None)")

    last = calc_df.iloc[-1]

    result = {
        "symbol": symbol,
        "period": period,
        "db_period": db_period,
        "candles_count": len(data_df),
        "closed_candles_only": closed_candles_only,
        "last_candle_time": int(last["Date"]),
        "last_candle_time_text": tools.int_to_datetime(int(last["Date"])),
        "last_close": float(last["Close"]) if pd.notna(last["Close"]) else None,
        "last_tenkan": float(last["Tenkan_sen"]) if pd.notna(last["Tenkan_sen"]) else None,
        "last_kijun": float(last["Kijun_sen"]) if pd.notna(last["Kijun_sen"]) else None,
        "last_senkou_a": float(last["Senkou_Span_A"]) if pd.notna(last["Senkou_Span_A"]) else None,
        "last_senkou_b": float(last["Senkou_Span_B"]) if pd.notna(last["Senkou_Span_B"]) else None,
        "trend": str(trend),
        "time_result": analyze_obj.get_time(),
        "time_result_text": tools.int_to_datetime(analyze_obj.get_time()),
        "cross_tenkan_kijun": str(ichi_result.crossover_result_tenkansen_kiusen),
        "cross_tenkan_kijun_time": ichi_result.time_of_cross_tenkansen_kiusen,
        "cross_tenkan_kijun_time_text": tools.int_to_datetime(ichi_result.time_of_cross_tenkansen_kiusen)
        if ichi_result.time_of_cross_tenkansen_kiusen is not None
        else None,
        "cross_price_kijun": str(ichi_result.crossover_result_price_kiusen),
        "cross_price_kijun_time": ichi_result.time_of_cross_price_kiusen,
        "cross_price_kijun_time_text": tools.int_to_datetime(ichi_result.time_of_cross_price_kiusen)
        if ichi_result.time_of_cross_price_kiusen is not None
        else None,
        "cross_price_cloud": str(ichi_result.crossover_price_senokuspan),
        "cross_price_cloud_time": ichi_result.time_of_cross_price_senokuspan,
        "cross_price_cloud_time_text": tools.int_to_datetime(ichi_result.time_of_cross_price_senokuspan)
        if ichi_result.time_of_cross_price_senokuspan is not None
        else None,
        "buy_signal": any(
            [
                ichi_result.crossover_result_tenkansen_kiusen
                == ichi.ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_gory,
                ichi_result.crossover_result_price_kiusen
                == ichi.ichi_crossover_price_kiusen_result_enum.Przeciecie_do_gory,
                ichi_result.crossover_price_senokuspan
                == ichi.ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_gory,
            ]
        ),
        "sell_signal": any(
            [
                ichi_result.crossover_result_tenkansen_kiusen
                == ichi.ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_dolu,
                ichi_result.crossover_result_price_kiusen
                == ichi.ichi_crossover_price_kiusen_result_enum.Przeciecie_do_dolu,
                ichi_result.crossover_price_senokuspan
                == ichi.ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_dolu,
            ]
        ),
    }

    print("=== TEST ICHIMOKU (DB) ===")
    for key, value in result.items():
        print(f"{key}: {value}")

    return result


def print_ichi_table_from_database(
    symbol: str = "UK100",
    period: str = "M5",
    candles_count: int = 300,
    tenkansen_period: int = 9,
    kiusen_period: int = 26,
    senokuspanB_period: int = 52,
    closed_candles_only: bool = True,
) -> None:
    ordered_rows = _prepare_candles(symbol, period, candles_count, closed_candles_only)
    db_period = _normalize_db_period(period)

    ichi_obj = ichi.ichimoku_object()
    data_df = ichi_obj.get_data_from_candle_array(ordered_rows)
    calc_df = ichi_obj.calculate_ichimoku(
        data_df,
        tenkansen=tenkansen_period,
        kiusen=kiusen_period,
        senokuspan=senokuspanB_period,
    )

    print("=== ICHIMOKU DLA KAZDEJ SWIECY (DB) ===")
    print(
        f"symbol={symbol} period={period} db_period={db_period} candles={len(calc_df)} "
        f"closed_candles_only={closed_candles_only}"
    )
    print("time | close | tenkan | kijun | senkou_a | senkou_b | cross_tk_kj | cross_price_kj | cross_price_cloud")

    for idx in range(len(calc_df)):
        row = calc_df.iloc[idx]

        row_time = int(row["Date"])
        close_value = float(row["Close"]) if pd.notna(row["Close"]) else float("nan")
        tenkan = float(row["Tenkan_sen"]) if pd.notna(row["Tenkan_sen"]) else float("nan")
        kijun = float(row["Kijun_sen"]) if pd.notna(row["Kijun_sen"]) else float("nan")
        senkou_a = float(row["Senkou_Span_A"]) if pd.notna(row["Senkou_Span_A"]) else float("nan")
        senkou_b = float(row["Senkou_Span_B"]) if pd.notna(row["Senkou_Span_B"]) else float("nan")

        cross_tk_kj = "BRAK"
        if idx > 0:
            prev = calc_df.iloc[idx - 1]
            prev_tk = prev.get("Tenkan_sen")
            prev_kj = prev.get("Kijun_sen")
            curr_tk = row.get("Tenkan_sen")
            curr_kj = row.get("Kijun_sen")
            if all(pd.notna(value) for value in [prev_tk, prev_kj, curr_tk, curr_kj]):
                if prev_tk <= prev_kj and curr_tk > curr_kj:
                    cross_tk_kj = "WZROST"
                elif prev_tk >= prev_kj and curr_tk < curr_kj:
                    cross_tk_kj = "SPADEK"

        cross_price_kj = "BRAK"
        if pd.notna(kijun):
            open_price = float(row["Open"])
            if open_price <= kijun < close_value:
                cross_price_kj = "WZROST"
            elif open_price >= kijun > close_value:
                cross_price_kj = "SPADEK"

        cross_price_cloud = "BRAK"
        open_price = float(row["Open"])
        crosses_up = False
        crosses_down = False
        if pd.notna(senkou_a):
            crosses_up = crosses_up or (open_price <= senkou_a < close_value)
            crosses_down = crosses_down or (open_price >= senkou_a > close_value)
        if pd.notna(senkou_b):
            crosses_up = crosses_up or (open_price <= senkou_b < close_value)
            crosses_down = crosses_down or (open_price >= senkou_b > close_value)

        if crosses_up:
            cross_price_cloud = "WZROST"
        elif crosses_down:
            cross_price_cloud = "SPADEK"

        print(
            f"{tools.int_to_datetime(row_time)} | "
            f"{close_value:.2f} | "
            f"{tenkan:.6f} | "
            f"{kijun:.6f} | "
            f"{senkou_a:.6f} | "
            f"{senkou_b:.6f} | "
            f"{cross_tk_kj} | "
            f"{cross_price_kj} | "
            f"{cross_price_cloud}"
        )


if __name__ == "__main__":
    print_ichi_table_from_database(symbol="UK100", period="M5", candles_count=300)
    test_ichi_on_database_data(symbol="UK100", period="M5", candles_count=300)
