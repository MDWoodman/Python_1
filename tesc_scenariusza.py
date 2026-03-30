from __future__ import annotations

import pandas as pd

import scenariusz
import zakoncz_scenariusz
import tools
import config.conf as cnf
from audit_log import log_trade_audit_event
from database import database as db
from wskazniki import adx__chat as adx
from wskazniki import ichi__chat as ichi
from wskazniki import mcad__chat as mcd


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

    # DB returns newest first, so reverse for indicator calculations.
    ordered_rows = list(reversed(raw_rows))

    if closed_candles_only:
        if len(ordered_rows) < 2:
            raise ValueError("Za malo swiec, aby pominac ostatnia (otwarta) swiece")
        ordered_rows = ordered_rows[:-1]

    return ordered_rows


def _build_ichimoku_lists(
    ichi_result_data: ichi.ichi_result_object,
) -> tuple[list[str], list[str]]:
    result_k: list[str] = []
    result_s: list[str] = []

    def append_result(target: list[str], result_name: str, time_value: int | None) -> None:
        if time_value is None:
            return
        time_int = int(time_value)
        target.append(result_name + " ," + str(time_int) + " , " + str(tools.int_to_datetime(time_int)))

    if ichi_result_data.crossover_result_tenkansen_kiusen == ichi.ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_gory:
        append_result(
            result_k,
            str(ichi_result_data.crossover_result_tenkansen_kiusen),
            ichi_result_data.time_of_cross_tenkansen_kiusen,
        )

    if ichi_result_data.crossover_result_price_kiusen == ichi.ichi_crossover_price_kiusen_result_enum.Przeciecie_do_gory:
        append_result(
            result_k,
            str(ichi_result_data.crossover_result_price_kiusen),
            ichi_result_data.time_of_cross_price_kiusen,
        )

    if ichi_result_data.crossover_price_senokuspan == ichi.ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_gory:
        append_result(
            result_k,
            "ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_gory",
            ichi_result_data.time_of_cross_price_senokuspan,
        )

    if ichi_result_data.crossover_result_tenkansen_kiusen == ichi.ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_dolu:
        append_result(
            result_s,
            str(ichi_result_data.crossover_result_tenkansen_kiusen),
            ichi_result_data.time_of_cross_tenkansen_kiusen,
        )

    if ichi_result_data.crossover_result_price_kiusen == ichi.ichi_crossover_price_kiusen_result_enum.Przeciecie_do_dolu:
        append_result(
            result_s,
            str(ichi_result_data.crossover_result_price_kiusen),
            ichi_result_data.time_of_cross_price_kiusen,
        )

    if ichi_result_data.crossover_price_senokuspan == ichi.ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_dolu:
        append_result(
            result_s,
            "ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_dolu",
            ichi_result_data.time_of_cross_price_senokuspan,
        )

    return result_k, result_s


def _raw_cross_for_adx_df(adx_df: pd.DataFrame) -> str:
    if adx_df is None or len(adx_df) < 2:
        return "BRAK"

    prev = adx_df.iloc[-2]
    curr = adx_df.iloc[-1]

    prev_plus = prev.get("+DI")
    prev_minus = prev.get("-DI")
    curr_plus = curr.get("+DI")
    curr_minus = curr.get("-DI")

    if any(pd.isna(value) for value in [prev_plus, prev_minus, curr_plus, curr_minus]):
        return "BRAK"

    if prev_plus <= prev_minus and curr_plus > curr_minus:
        return "WZROST"
    if prev_plus >= prev_minus and curr_plus < curr_minus:
        return "SPADEK"
    return "BRAK"


def _filtered_adx_signal(adx_analyze_result_tuple) -> str:
    if adx_analyze_result_tuple is None or adx_analyze_result_tuple[0] is None:
        return "BRAK"

    result = adx_analyze_result_tuple[0].get_result()
    if result == adx.adx_result_enum.Wzrost_przeciecie:
        return "BUY"
    if result == adx.adx_result_enum.Spadek_przeciecie:
        return "SELL"
    return "BRAK"


def _trade_signal_from_raw_cross(raw_cross: str) -> str:
    if raw_cross == "WZROST":
        return "BUY"
    if raw_cross == "SPADEK":
        return "SELL"
    return "BRAK"


def _adx_tuple_from_raw_cross(
    raw_cross: str,
    candle_time: int,
    symbol: str,
    period: str,
    fallback_trend,
):
    if raw_cross == "WZROST":
        adx_result = adx.adx_result_enum.Wzrost_przeciecie
    elif raw_cross == "SPADEK":
        adx_result = adx.adx_result_enum.Spadek_przeciecie
    else:
        adx_result = adx.adx_result_enum.Boczny

    trend = fallback_trend if fallback_trend is not None else adx.Trend.NEITHER
    adx_obj = adx.adx_analyze_result_object(
        time=int(candle_time),
        symbol=symbol,
        period=period,
        result=adx_result,
        trend=trend,
    )
    return adx_obj, trend


def _is_signal_within_max_time(candle_time: int, signal_time: int | None, max_time_result_minutes: int | None) -> bool:
    if signal_time is None:
        return False
    if max_time_result_minutes is None:
        return True

    diff_ms = int(candle_time) - int(signal_time)
    if diff_ms < 0:
        return False

    return diff_ms <= int(max_time_result_minutes) * 60_000


def _latest_ichimoku_event(
    ichi_result_data: ichi.ichi_result_object | None,
) -> tuple[str, str, int | None]:
    if ichi_result_data is None:
        return "BRAK", "BRAK", None

    events: list[tuple[int, str, str]] = []

    def add_event(time_value: int | None, signal: str, event_name: str) -> None:
        if time_value is None:
            return
        events.append((int(time_value), signal, event_name))

    if ichi_result_data.crossover_result_tenkansen_kiusen == ichi.ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_gory:
        add_event(ichi_result_data.time_of_cross_tenkansen_kiusen, "BUY", "TENKAN_KIJUN_UP")
    elif ichi_result_data.crossover_result_tenkansen_kiusen == ichi.ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_dolu:
        add_event(ichi_result_data.time_of_cross_tenkansen_kiusen, "SELL", "TENKAN_KIJUN_DOWN")

    if ichi_result_data.crossover_result_price_kiusen == ichi.ichi_crossover_price_kiusen_result_enum.Przeciecie_do_gory:
        add_event(ichi_result_data.time_of_cross_price_kiusen, "BUY", "PRICE_KIJUN_UP")
    elif ichi_result_data.crossover_result_price_kiusen == ichi.ichi_crossover_price_kiusen_result_enum.Przeciecie_do_dolu:
        add_event(ichi_result_data.time_of_cross_price_kiusen, "SELL", "PRICE_KIJUN_DOWN")

    if ichi_result_data.crossover_price_senokuspan == ichi.ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_gory:
        add_event(ichi_result_data.time_of_cross_price_senokuspan, "BUY", "PRICE_CLOUD_UP")
    elif ichi_result_data.crossover_price_senokuspan == ichi.ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_dolu:
        add_event(ichi_result_data.time_of_cross_price_senokuspan, "SELL", "PRICE_CLOUD_DOWN")

    if not events:
        return "BRAK", "BRAK", None

    latest_time, latest_signal, latest_event_name = max(events, key=lambda item: item[0])
    return latest_signal, latest_event_name, latest_time


def _calculate_sl_tp(entry_price: float, side: str, sl_distance: float, tp_distance: float) -> tuple[float, float]:
    side_upper = str(side or "").upper()
    if side_upper == "BUY":
        return float(entry_price - sl_distance), float(entry_price + tp_distance)
    if side_upper == "SELL":
        return float(entry_price + sl_distance), float(entry_price - tp_distance)
    raise ValueError(f"Nieznany typ transakcji: {side}")


def _check_sl_tp_hit(side: str, candle_high: float, candle_low: float, stop_loss: float, take_profit: float) -> str | None:
    side_upper = str(side or "").upper()
    if side_upper == "BUY":
        if candle_low <= float(stop_loss):
            return "SL"
        if candle_high >= float(take_profit):
            return "TP"
        return None

    if side_upper == "SELL":
        if candle_high >= float(stop_loss):
            return "SL"
        if candle_low <= float(take_profit):
            return "TP"
        return None

    return None


def test_scenariusz_on_database_data(
    symbol: str = "UK100",
    period: str = "M5",
    candles_count: int = 350,
    adx_window: int = 14,
    adx_threshold: float = 25,
    short_window: int = 12,
    long_window: int = 26,
    signal_window: int = 9,
    angle: float = 45,
    tenkansen_period: int = 9,
    kiusen_period: int = 26,
    senokuspanB_period: int = 52,
    last_n_candles: int = 30,
    max_time_result_minutes: int = cnf.MAX_TIME_RESULT,
    closed_candles_only: bool = True,
    sl_distance: float = 40.0,
    tp_distance: float = 80.0,
) -> dict:
    ordered_rows = _prepare_candles(symbol, period, candles_count, closed_candles_only)
    db_period = _normalize_db_period(period)

    min_required = max(
        3,
        int(adx_window or 14) + 1,
        int(long_window or 26) + int(signal_window or 9),
        int(senokuspanB_period or 52),
    )
    if len(ordered_rows) < min_required:
        raise ValueError(
            f"Za malo danych do analizy petlowej z historia (min {min_required} swiec, jest {len(ordered_rows)})."
        )

    adx_obj = adx.adx_object()
    ichi_obj = ichi.ichimoku_object()

    # Analiza rosnaco: kazda iteracja bierze dane od pierwszej swiecy do i-tej.
    candle_signals = []
    latest_adx_tuple: tuple[adx.adx_analyze_result_object | None, adx.Trend | None] = (None, None)
    latest_mcad_obj = None
    latest_ichi_k: list[str] = []
    latest_ichi_s: list[str] = []
    latest_scenario: dict = {"signal": None, "scenario_number": None, "scenario_conditions": "BRAK"}
    latest_adx_raw_cross = "BRAK"
    latest_adx_filtered_signal = "BRAK"
    latest_adx_signal = "BRAK"
    latest_close_result: dict = {"close": False, "scenario_number": None, "scenario_conditions": "BRAK"}
    mcad_last_event_id: tuple[str, int] | None = None
    mcad_event_emit_count = 0
    ichi_last_event_id: tuple[str, str, int] | None = None
    ichi_event_emit_count = 0
    last_adx_buy_time: int | None = None
    last_adx_sell_time: int | None = None
    opened_transaction_type: str | None = None
    opened_entry_price: float | None = None
    opened_stop_loss: float | None = None
    opened_take_profit: float | None = None

    # Start po okresie rozgrzewki, aby kazda iteracja miala poprzednie swiece dla ADX/MCAD/Ichimoku.
    for i in range(min_required, len(ordered_rows) + 1):
        subset_rows = ordered_rows[:i]
        subset_df = adx_obj.get_data_from_candle_array(subset_rows)

        adx_analyze_result_tuple = adx.analyze_adx_candles(
            subset_df,
            adx_window,
            adx_threshold,
            symbol,
            period,
        )

        adx_calc_df = adx_obj.calculate_adx(subset_df, period=adx_window)
        adx_raw_cross = _raw_cross_for_adx_df(adx_calc_df)
        adx_filtered_signal = _filtered_adx_signal(adx_analyze_result_tuple)
        adx_raw_tuple = _adx_tuple_from_raw_cross(
            raw_cross=adx_raw_cross,
            candle_time=int(subset_rows[-1][0]),
            symbol=symbol,
            period=period,
            fallback_trend=adx_analyze_result_tuple[1],
        )

        current_candle_time = int(subset_rows[-1][0])

        if adx_filtered_signal == "BUY":
            if adx_analyze_result_tuple[0] is not None and adx_analyze_result_tuple[0].get_time() is not None:
                last_adx_buy_time = int(adx_analyze_result_tuple[0].get_time())
            else:
                last_adx_buy_time = current_candle_time
        elif adx_filtered_signal == "SELL":
            if adx_analyze_result_tuple[0] is not None and adx_analyze_result_tuple[0].get_time() is not None:
                last_adx_sell_time = int(adx_analyze_result_tuple[0].get_time())
            else:
                last_adx_sell_time = current_candle_time

        adx_for_scenario = adx_analyze_result_tuple
        adx_trend_for_scenario = adx_analyze_result_tuple[1] if adx_analyze_result_tuple[1] is not None else adx.Trend.NEITHER

        buy_active = _is_signal_within_max_time(current_candle_time, last_adx_buy_time, max_time_result_minutes)
        sell_active = _is_signal_within_max_time(current_candle_time, last_adx_sell_time, max_time_result_minutes)

        if buy_active and (not sell_active or int(last_adx_buy_time) >= int(last_adx_sell_time)):
            adx_obj_for_scenario = adx.adx_analyze_result_object(
                time=int(last_adx_buy_time),
                symbol=symbol,
                period=period,
                result=adx.adx_result_enum.Wzrost_przeciecie,
                trend=adx_trend_for_scenario,
            )
            adx_for_scenario = (adx_obj_for_scenario, adx_trend_for_scenario)
        elif sell_active and (not buy_active or int(last_adx_sell_time) > int(last_adx_buy_time)):
            adx_obj_for_scenario = adx.adx_analyze_result_object(
                time=int(last_adx_sell_time),
                symbol=symbol,
                period=period,
                result=adx.adx_result_enum.Spadek_przeciecie,
                trend=adx_trend_for_scenario,
            )
            adx_for_scenario = (adx_obj_for_scenario, adx_trend_for_scenario)
        
        mcad_analyze_result_obj, _ = mcd.analyze_mcad_candles(
            subset_df,
            short_window=short_window,
            long_window=long_window,
            signal_window=signal_window,
            angle=angle,
            symbol=symbol,
            period=period,
        )

        tail_df = subset_df.tail(int(last_n_candles)).copy()
        ichi_analyze_result_obj, _ = ichi.analyze_ichimoku_candles(
            subset_df,
            tail_df,
            tenkansen_period=tenkansen_period,
            kiusen_period=kiusen_period,
            senokuspanB_period=senokuspanB_period,
            symbol=symbol,
            period=period,
        )

        if ichi_analyze_result_obj is not None:
            ichimoku_result_k, ichimoku_result_s = _build_ichimoku_lists(ichi_analyze_result_obj.get_result())
        else:
            ichimoku_result_k, ichimoku_result_s = [], []

        ichimoku_signal = "BRAK"
        ichimoku_event = "BRAK"
        if ichi_analyze_result_obj is not None:
            raw_ichi_signal, raw_ichi_event, raw_ichi_time = _latest_ichimoku_event(ichi_analyze_result_obj.get_result())
            if raw_ichi_signal in ("BUY", "SELL") and raw_ichi_time is not None:
                current_ichi_event_id = (raw_ichi_signal, raw_ichi_event, int(raw_ichi_time))

                if current_ichi_event_id != ichi_last_event_id:
                    ichi_last_event_id = current_ichi_event_id
                    ichi_event_emit_count = 0

                if ichi_event_emit_count < 3:
                    ichimoku_signal = raw_ichi_signal
                    ichimoku_event = raw_ichi_event
                    ichi_event_emit_count += 1

        scenario_result = scenariusz.get_trade_signal(
            adx_analyze_result_obj=adx_for_scenario,
            mcad_analyze_result_obj=mcad_analyze_result_obj,
            ichimoku_result_k=ichimoku_result_k,
            ichimoku_result_s=ichimoku_result_s,
            period=period,
            max_time_result_minutes=max_time_result_minutes,
        )

        scenario_signal = scenario_result.get("signal")
        scenario_number = scenario_result.get("scenario_number")
        scenario_conditions = scenario_result.get("scenario_conditions")

        if scenario_signal in ("BUY", "SELL"):
            log_trade_audit_event(
                symbol=symbol,
                event_type="OPEN_SIGNAL",
                signal=scenario_signal,
                broker_time_ms=current_candle_time,
                open_scenario_number=scenario_number,
                scenario_conditions=scenario_conditions,
            )
            if opened_transaction_type is None:
                opened_transaction_type = str(scenario_signal)
                opened_entry_price = float(subset_rows[-1][4])
                opened_stop_loss, opened_take_profit = _calculate_sl_tp(
                    entry_price=opened_entry_price,
                    side=opened_transaction_type,
                    sl_distance=float(sl_distance),
                    tp_distance=float(tp_distance),
                )

        close_result = {"close": False, "scenario_number": None, "scenario_conditions": "BRAK"}
        close_reason = "BRAK"

        if (
            opened_transaction_type in ("BUY", "SELL")
            and opened_stop_loss is not None
            and opened_take_profit is not None
        ):
            sl_tp_hit = _check_sl_tp_hit(
                side=opened_transaction_type,
                candle_high=float(subset_rows[-1][2]),
                candle_low=float(subset_rows[-1][3]),
                stop_loss=float(opened_stop_loss),
                take_profit=float(opened_take_profit),
            )
            if sl_tp_hit == "SL":
                close_result = {
                    "close": True,
                    "scenario_number": 90,
                    "scenario_conditions": (
                        f"C90 CLOSE {opened_transaction_type} BY STOP LOSS | "
                        f"ENTRY={opened_entry_price}; SL={opened_stop_loss}; TP={opened_take_profit}"
                    ),
                }
                close_reason = "SL"
            elif sl_tp_hit == "TP":
                close_result = {
                    "close": True,
                    "scenario_number": 91,
                    "scenario_conditions": (
                        f"C91 CLOSE {opened_transaction_type} BY TAKE PROFIT | "
                        f"ENTRY={opened_entry_price}; SL={opened_stop_loss}; TP={opened_take_profit}"
                    ),
                }
                close_reason = "TP"

        if opened_transaction_type in ("BUY", "SELL"):
            if close_result.get("close") is not True:
                close_result = zakoncz_scenariusz.get_close_signal(
                    opened_transaction_type=opened_transaction_type,
                    adx_analyze_result_obj=adx_for_scenario,
                    mcad_analyze_result_obj=mcad_analyze_result_obj,
                    ichimoku_result_k=ichimoku_result_k,
                    ichimoku_result_s=ichimoku_result_s,
                    period=period,
                    max_time_result_minutes=max_time_result_minutes,
                )

            if close_result.get("close") is True:
                close_reason = close_reason if close_reason != "BRAK" else "SCENARIO"
                log_trade_audit_event(
                    symbol=symbol,
                    event_type="CLOSE_SIGNAL" if close_reason == "SCENARIO" else f"CLOSE_{close_reason}",
                    signal=opened_transaction_type,
                    broker_time_ms=current_candle_time,
                    close_scenario_number=close_result.get("scenario_number"),
                    scenario_conditions=close_result.get("scenario_conditions"),
                )
                opened_transaction_type = None
                opened_entry_price = None
                opened_stop_loss = None
                opened_take_profit = None

        # ADX_RAW is the primary ADX signal used in this test.
        adx_signal = _trade_signal_from_raw_cross(adx_raw_cross)

        mcad_signal = "BRAK"
        mcad_trade_signal = "BRAK"
        if mcad_analyze_result_obj is not None:
            mcad_signal = str(mcad_analyze_result_obj.get_result())
            raw_mcad_cross = mcad_analyze_result_obj.get_raw_cross()
            raw_mcad_cross_time = mcad_analyze_result_obj.get_raw_cross_time()

            if raw_mcad_cross in ("WZROST", "SPADEK") and raw_mcad_cross_time is not None:
                current_event_id = (raw_mcad_cross, int(raw_mcad_cross_time))

                if current_event_id != mcad_last_event_id:
                    mcad_last_event_id = current_event_id
                    mcad_event_emit_count = 0

                if mcad_event_emit_count < 3:
                    mcad_trade_signal = "BUY" if raw_mcad_cross == "WZROST" else "SELL"
                    mcad_event_emit_count += 1

        candle_time = int(subset_rows[-1][0])
        prev_candle_time = int(subset_rows[-2][0])
        prev_candle_close = float(subset_rows[-2][4])
        candle_signals.append({
            "time": candle_time,
            "time_text": tools.int_to_datetime(candle_time),
            "prev_time": prev_candle_time,
            "prev_time_text": tools.int_to_datetime(prev_candle_time),
            "prev_close": prev_candle_close,
            "adx_signal": adx_signal,
            "adx_raw_cross": adx_raw_cross,
            "adx_filtered_signal": adx_filtered_signal,
            "mcad_signal": mcad_signal,
            "mcad_trade_signal": mcad_trade_signal,
            "ichimoku_signal": ichimoku_signal,
            "ichimoku_event": ichimoku_event,
            "scenario_signal": scenario_signal or "BRAK",
            "scenario_number": scenario_number or "BRAK",
            "scenario_conditions": scenario_conditions or "BRAK",
            "close_signal": "TAK" if close_result.get("close") is True else "NIE",
            "close_scenario_number": close_result.get("scenario_number") or "BRAK",
            "close_scenario_conditions": close_result.get("scenario_conditions") or "BRAK",
            "close_reason": close_reason,
            "opened_entry_price": opened_entry_price if opened_entry_price is not None else "BRAK",
            "opened_sl": opened_stop_loss if opened_stop_loss is not None else "BRAK",
            "opened_tp": opened_take_profit if opened_take_profit is not None else "BRAK",
        })

        latest_adx_tuple = adx_analyze_result_tuple
        latest_mcad_obj = mcad_analyze_result_obj
        latest_ichi_k = ichimoku_result_k
        latest_ichi_s = ichimoku_result_s
        latest_scenario = scenario_result
        latest_close_result = close_result
        latest_adx_raw_cross = adx_raw_cross
        latest_adx_filtered_signal = adx_filtered_signal
        latest_adx_signal = adx_signal
    
    print("\n=== SYGNAAŁY DLA KAŻDEJ ŚWIECY ===")
    for signal in candle_signals:
        print(
            f"Czas: {signal['time_text']} |  "
            f"| SCENARIO_SIGNAL: {signal['scenario_signal']} "
            f"| SCENARIO_NUMBER: {signal['scenario_number']} "
            f"| SCENARIO_CONDITIONS: {signal['scenario_conditions']} "
            f"| CLOSE: {signal['close_signal']} "
            f"| CLOSE_SCENARIO: {signal['close_scenario_number']} "
            f"| CLOSE_REASON: {signal['close_reason']} "
            f"| ENTRY/SL/TP: {signal['opened_entry_price']}/{signal['opened_sl']}/{signal['opened_tp']}"
                )

    last_candle_time = int(ordered_rows[-1][0])
    result = {
        "symbol": symbol,
        "adx_signal": latest_adx_signal,
        "adx_signal_source": "ADX_RAW",
        "adx_raw_cross": latest_adx_raw_cross,
        "adx_filtered_signal": latest_adx_filtered_signal,
        "mcad_signal": str(latest_mcad_obj.get_result()) if latest_mcad_obj is not None else "BRAK",
        "ichimoku_signal": "BUY" if latest_ichi_k else ("SELL" if latest_ichi_s else "BRAK"),
        "period": period,
        "db_period": db_period,
        "candles_count": len(ordered_rows),
        "closed_candles_only": closed_candles_only,
        "last_candle_time": last_candle_time,
        "last_candle_time_text": tools.int_to_datetime(last_candle_time),
        "adx_result": latest_adx_raw_cross,
        "adx_trend": str(latest_adx_tuple[1]) if latest_adx_tuple[1] is not None else "BRAK",
        "mcad_result": str(latest_mcad_obj.get_result()) if latest_mcad_obj is not None else "BRAK",
        "ichimoku_result_k": latest_ichi_k,
        "ichimoku_result_s": latest_ichi_s,
        "scenario_signal": latest_scenario.get("signal"),
        "scenario_number": latest_scenario.get("scenario_number"),
        "scenario_conditions": latest_scenario.get("scenario_conditions"),
        "close_signal": latest_close_result.get("close"),
        "close_scenario_number": latest_close_result.get("scenario_number"),
        "close_scenario_conditions": latest_close_result.get("scenario_conditions"),
        "sl_distance": float(sl_distance),
        "tp_distance": float(tp_distance),
        "buy_signal": latest_scenario.get("signal") == "BUY",
        "sell_signal": latest_scenario.get("signal") == "SELL",
    }



    return result


def print_scenariusz_table_from_database(
    symbol: str = "UK100",
    period: str = "H1",
    candles_count: int = 350,
    closed_candles_only: bool = True,
) -> pd.DataFrame:
    ordered_rows = _prepare_candles(symbol, period, candles_count, closed_candles_only)
    df = pd.DataFrame(
        {
            "Date": [row[0] for row in ordered_rows],
            "Open": [row[1] for row in ordered_rows],
            "High": [row[2] for row in ordered_rows],
            "Low": [row[3] for row in ordered_rows],
            "Close": [row[4] for row in ordered_rows],
            "Volume": [row[5] for row in ordered_rows],
        }
    )

   
    print("time | open | high | low | close | volume")

    for idx in range(len(df)):
        row = df.iloc[idx]
        print(
            f"{tools.int_to_datetime(int(row['Date']))} | "
            f"{float(row['Open']):.2f} | "
            f"{float(row['High']):.2f} | "
            f"{float(row['Low']):.2f} | "
            f"{float(row['Close']):.2f} | "
            f"{float(row['Volume']):.2f}"
        )

    return df


if __name__ == "__main__":
    #print_scenariusz_table_from_database(symbol="UK100", period="M5", candles_count=350)
    test_scenariusz_on_database_data(symbol="STOXX50", period="M5", candles_count=350)
