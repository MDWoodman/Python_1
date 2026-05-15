from __future__ import annotations

import re
from typing import Any

from wskazniki import adx__chat as adxcht
from wskazniki import mcad__chat as mcad


M5_SCENARIO_LIMITS = {"strict": 20, "medium": 35, "relaxed": 50}
MCAD_FRESH_CROSS_MAX_MINUTES = 15
BULLISH_CANDLE_PATTERNS = {"Hammer", "BullishEngulfing", "MorningStar"}
BEARISH_CANDLE_PATTERNS = {"ShootingStar", "BearishEngulfing", "EveningStar"}


def _has_ichi_buy_signal(ichimoku_result_k: list[str]) -> bool:
    if not ichimoku_result_k:
        return False

    buy_keywords = [
        "Przeciecie_do_gory",
        "price_senokuspan_result_enum.Przeciecie_do_gory",
        "price_kiusen_result_enum.Przeciecie_do_gory",
        "tenkansen_kiusen_result_enum.Przeciecie_do_gory",
    ]
    return any(any(keyword in entry for keyword in buy_keywords) for entry in ichimoku_result_k)


def _has_ichi_sell_signal(ichimoku_result_s: list[str]) -> bool:
    if not ichimoku_result_s:
        return False

    sell_keywords = [
        "Przeciecie_do_dolu",
        "price_senokuspan_result_enum.Przeciecie_do_dolu",
        "price_kiusen_result_enum.Przeciecie_do_dolu",
        "tenkansen_kiusen_result_enum.Przeciecie_do_dolu",
    ]
    return any(any(keyword in entry for keyword in sell_keywords) for entry in ichimoku_result_s)


def _has_tenkansen_kijun_buy_cross(ichimoku_result_k: list[str]) -> bool:
    if not ichimoku_result_k:
        return False
    return any("tenkansen_kiusen_result_enum.Przeciecie_do_gory" in entry for entry in ichimoku_result_k)


def _has_price_kijun_buy_cross(ichimoku_result_k: list[str]) -> bool:
    if not ichimoku_result_k:
        return False
    return any("price_kiusen_result_enum.Przeciecie_do_gory" in entry for entry in ichimoku_result_k)


def _has_cloud_buy_breakout(ichimoku_result_k: list[str]) -> bool:
    if not ichimoku_result_k:
        return False
    return any("price_senokuspan_result_enum.Przeciecie_do_gory" in entry for entry in ichimoku_result_k)


def _has_tenkansen_kijun_sell_cross(ichimoku_result_s: list[str]) -> bool:
    if not ichimoku_result_s:
        return False
    return any("tenkansen_kiusen_result_enum.Przeciecie_do_dolu" in entry for entry in ichimoku_result_s)


def _has_price_kijun_sell_cross(ichimoku_result_s: list[str]) -> bool:
    if not ichimoku_result_s:
        return False
    return any("price_kiusen_result_enum.Przeciecie_do_dolu" in entry for entry in ichimoku_result_s)


def _has_cloud_sell_breakout(ichimoku_result_s: list[str]) -> bool:
    if not ichimoku_result_s:
        return False
    return any("price_senokuspan_result_enum.Przeciecie_do_dolu" in entry for entry in ichimoku_result_s)


def _is_ichi_buy_strong(ichimoku_price_vs_cloud: str | None) -> bool:
    return str(ichimoku_price_vs_cloud or "").strip().lower() == "above_cloud"


def _is_ichi_buy_weak(ichimoku_price_vs_cloud: str | None) -> bool:
    normalized = str(ichimoku_price_vs_cloud or "").strip().lower()
    return normalized in {"below_cloud", "inside_cloud"}


def _is_ichi_sell_strong(ichimoku_price_vs_cloud: str | None) -> bool:
    return str(ichimoku_price_vs_cloud or "").strip().lower() == "below_cloud"


def _is_ichi_sell_weak(ichimoku_price_vs_cloud: str | None) -> bool:
    normalized = str(ichimoku_price_vs_cloud or "").strip().lower()
    return normalized in {"above_cloud", "inside_cloud"}


def _is_price_inside_cloud(ichimoku_price_vs_cloud: str | None) -> bool:
    """Helper function to check if price is inside the Ichimoku cloud."""
    return str(ichimoku_price_vs_cloud or "").strip().lower() == "inside_cloud"


def _extract_ichi_buy_times(ichimoku_result_k: list[str]) -> list[int]:
    times: list[int] = []
    for entry in ichimoku_result_k:
        matches = re.findall(r"\d{10,13}", entry)
        if not matches:
            continue
        try:
            times.append(int(matches[0]))
        except ValueError:
            continue
    return times


def _extract_ichi_sell_times(ichimoku_result_s: list[str]) -> list[int]:
    times: list[int] = []
    for entry in ichimoku_result_s:
        matches = re.findall(r"\d{10,13}", entry)
        if not matches:
            continue
        try:
            times.append(int(matches[0]))
        except ValueError:
            continue
    return times


def _are_signals_within_max_time(
    adx_analyze_result_obj: tuple[Any, Any],
    mcad_analyze_result_obj: Any,
    ichimoku_result_k: list[str],
    max_time_result_minutes: int,
) -> bool:
    if max_time_result_minutes is None:
        return True

    timestamps: list[int] = []

    if adx_analyze_result_obj is not None and adx_analyze_result_obj[0] is not None:
        adx_time = adx_analyze_result_obj[0].get_time()
        if adx_time is not None:
            timestamps.append(int(adx_time))

    if mcad_analyze_result_obj is not None:
        mcad_time = mcad_analyze_result_obj.get_time()
        if mcad_time is not None:
            timestamps.append(int(mcad_time))

    ichi_times = _extract_ichi_buy_times(ichimoku_result_k)
    if ichi_times:
        timestamps.append(max(ichi_times))

    if len(timestamps) < 2:
        return True

    max_diff_minutes = (max(timestamps) - min(timestamps)) / 60000.0
    return max_diff_minutes <= max_time_result_minutes


def _signal_time_window_minutes(
    adx_analyze_result_obj: tuple[Any, Any],
    mcad_analyze_result_obj: Any,
    ichimoku_result_k: list[str],
) -> float | None:
    timestamps: list[int] = []

    if adx_analyze_result_obj is not None and adx_analyze_result_obj[0] is not None:
        adx_time = adx_analyze_result_obj[0].get_time()
        if adx_time is not None:
            timestamps.append(int(adx_time))

    if mcad_analyze_result_obj is not None:
        mcad_time = mcad_analyze_result_obj.get_time()
        if mcad_time is not None:
            timestamps.append(int(mcad_time))

    ichi_times = _extract_ichi_buy_times(ichimoku_result_k)
    if ichi_times:
        timestamps.append(max(ichi_times))

    if len(timestamps) < 2:
        return None

    return (max(timestamps) - min(timestamps)) / 60000.0


def _signal_time_window_minutes_for_side(
    adx_analyze_result_obj: tuple[Any, Any],
    mcad_analyze_result_obj: Any,
    ichimoku_result_k: list[str],
    ichimoku_result_s: list[str],
    side: str,
) -> float | None:
    side_upper = str(side).upper()
    timestamps: list[int] = []

    if side_upper == "BUY":
        if _adx_is_buy(adx_analyze_result_obj):
            adx_time = adx_analyze_result_obj[0].get_time()
            if adx_time is not None:
                timestamps.append(int(adx_time))

        if _mcad_is_buy(mcad_analyze_result_obj):
            mcad_time = mcad_analyze_result_obj.get_time()
            if mcad_time is not None:
                timestamps.append(int(mcad_time))

        ichi_times = _extract_ichi_buy_times(ichimoku_result_k)
        if ichi_times:
            timestamps.append(max(ichi_times))

    if side_upper == "SELL":
        if _adx_is_sell(adx_analyze_result_obj):
            adx_time = adx_analyze_result_obj[0].get_time()
            if adx_time is not None:
                timestamps.append(int(adx_time))

        if _mcad_is_sell(mcad_analyze_result_obj):
            mcad_time = mcad_analyze_result_obj.get_time()
            if mcad_time is not None:
                timestamps.append(int(mcad_time))

        ichi_times = _extract_ichi_sell_times(ichimoku_result_s)
        if ichi_times:
            timestamps.append(max(ichi_times))

    if len(timestamps) < 2:
        return None

    return (max(timestamps) - min(timestamps)) / 60000.0


def _scenario_time_limits(period: str | None, max_time_result_minutes: int | None) -> dict[str, int]:
    period_upper = str(period or "").upper()

    if period_upper == "M5":
        return M5_SCENARIO_LIMITS.copy()

    # H1/H4 are slower systems. Keep wider windows to avoid rejecting valid confirmations.
    if period_upper == "H1":
        return {"strict": 180, "medium": 360, "relaxed": 720}
    if period_upper == "H4":
        return {"strict": 720, "medium": 1440, "relaxed": 2880}

    fallback = int(max_time_result_minutes) if max_time_result_minutes is not None else 510
    return {
        "strict": max(1, int(fallback * 0.5)),
        "medium": max(1, int(fallback * 0.75)),
        "relaxed": max(1, int(fallback)),
    }


def _within_window(window_minutes: float | None, limit_minutes: int) -> bool:
    if window_minutes is None:
        return True
    return window_minutes <= float(limit_minutes)


def _adx_is_buy(adx_analyze_result_obj: tuple[Any, Any]) -> bool:
    return (
        adx_analyze_result_obj is not None
        and adx_analyze_result_obj[0] is not None
        and adx_analyze_result_obj[0].get_result() == adxcht.adx_result_enum.Wzrost_przeciecie
    )


def _adx_is_sell(adx_analyze_result_obj: tuple[Any, Any]) -> bool:
    return (
        adx_analyze_result_obj is not None
        and adx_analyze_result_obj[0] is not None
        and adx_analyze_result_obj[0].get_result() == adxcht.adx_result_enum.Spadek_przeciecie
    )


def _adx_trend_increasing(adx_analyze_result_obj: tuple[Any, Any]) -> bool:
    return (
        adx_analyze_result_obj is not None
        and adx_analyze_result_obj[1] is not None
        and adx_analyze_result_obj[1] == adxcht.Trend.INCREASING
    )


def _adx_trend_decreasing(adx_analyze_result_obj: tuple[Any, Any]) -> bool:
    return (
        adx_analyze_result_obj is not None
        and adx_analyze_result_obj[1] is not None
        and adx_analyze_result_obj[1] == adxcht.Trend.DECREASING
    )


def _mcad_is_buy(mcad_analyze_result_obj: Any) -> bool:
    if mcad_analyze_result_obj is None:
        return False

    result = mcad_analyze_result_obj.get_result()
    if result == mcad.mcad_result_enum.Wzrost_przeciecie:
        return True

    # Safety fallback for mixed enum instances from legacy modules.
    return getattr(result, "name", "") == "Wzrost_przeciecie"


def _mcad_is_sell(mcad_analyze_result_obj: Any) -> bool:
    if mcad_analyze_result_obj is None:
        return False

    result = mcad_analyze_result_obj.get_result()
    if result == mcad.mcad_result_enum.Spadek_przeciecie:
        return True

    # Safety fallback for mixed enum instances from legacy modules.
    return getattr(result, "name", "") == "Spadek_przeciecie"


def _mcad_cross_is_fresh(mcad_analyze_result_obj: Any, max_age_minutes: int) -> bool:
    if mcad_analyze_result_obj is None:
        return False

    get_raw_cross_time = getattr(mcad_analyze_result_obj, "get_raw_cross_time", None)
    get_time = getattr(mcad_analyze_result_obj, "get_time", None)
    if get_raw_cross_time is None or get_time is None:
        return False

    raw_cross_time = get_raw_cross_time()
    last_time = get_time()
    if raw_cross_time is None or last_time is None:
        return False

    age_minutes = (int(last_time) - int(raw_cross_time)) / 60000.0
    if age_minutes < 0:
        return False

    return age_minutes <= float(max_age_minutes)


def _extract_candle_signal_data(candle_pattern_signal: dict[str, Any] | None) -> tuple[bool, bool, list[str], str | None]:
    if not candle_pattern_signal:
        return False, False, [], None

    raw_signal = candle_pattern_signal.get("signal")
    signal = str(raw_signal).upper() if raw_signal is not None else None
    patterns_raw = candle_pattern_signal.get("patterns")
    patterns = [str(item) for item in patterns_raw] if isinstance(patterns_raw, list) else []

    bullish_by_signal = signal == "BUY"
    bearish_by_signal = signal == "SELL"

    bullish_by_patterns = any(pattern in BULLISH_CANDLE_PATTERNS for pattern in patterns)
    bearish_by_patterns = any(pattern in BEARISH_CANDLE_PATTERNS for pattern in patterns)

    return bullish_by_signal or bullish_by_patterns, bearish_by_signal or bearish_by_patterns, patterns, signal


def get_trade_signal(
    adx_analyze_result_obj: tuple[Any, Any],
    mcad_analyze_result_obj: Any,
    ichimoku_result_k: list[str],
    ichimoku_result_s: list[str],
    ichimoku_price_vs_cloud: str | None,
    period: str | None,
    max_time_result_minutes: int | None = None,
    candle_pattern_signal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    limits = _scenario_time_limits(period, max_time_result_minutes)

    adx_buy = _adx_is_buy(adx_analyze_result_obj)
    adx_sell = _adx_is_sell(adx_analyze_result_obj)
    adx_inc = _adx_trend_increasing(adx_analyze_result_obj)
    adx_dec = _adx_trend_decreasing(adx_analyze_result_obj)


    mcad_buy = _mcad_is_buy(mcad_analyze_result_obj)
    mcad_sell = _mcad_is_sell(mcad_analyze_result_obj)
    mcad_fresh = _mcad_cross_is_fresh(mcad_analyze_result_obj, MCAD_FRESH_CROSS_MAX_MINUTES)

    ichi_buy = _has_ichi_buy_signal(ichimoku_result_k)
    ichi_sell = _has_ichi_sell_signal(ichimoku_result_s)
    ichi_tk_buy = _has_tenkansen_kijun_buy_cross(ichimoku_result_k)
    ichi_price_kijun_buy = _has_price_kijun_buy_cross(ichimoku_result_k)
    ichi_cloud_breakout_buy = _has_cloud_buy_breakout(ichimoku_result_k)
    ichi_tk_sell = _has_tenkansen_kijun_sell_cross(ichimoku_result_s)
    ichi_price_kijun_sell = _has_price_kijun_sell_cross(ichimoku_result_s)
    ichi_cloud_breakout_sell = _has_cloud_sell_breakout(ichimoku_result_s)
    ichi_buy_strong = _is_ichi_buy_strong(ichimoku_price_vs_cloud)
    ichi_buy_weak = _is_ichi_buy_weak(ichimoku_price_vs_cloud)
    ichi_sell_strong = _is_ichi_sell_strong(ichimoku_price_vs_cloud)
    ichi_sell_weak = _is_ichi_sell_weak(ichimoku_price_vs_cloud)
    candle_buy, candle_sell, candle_patterns, candle_signal = _extract_candle_signal_data(candle_pattern_signal)

    # Ambiguous Ichimoku state: skip entries when both sides are signaled at once.
    if ichi_buy and ichi_sell:
        return {
            "signal": None,
            "scenario_number": None,
            "scenario_conditions": (
                f"NO_MATCH | PERIOD={period}; LIMITS={limits}; AMBIGUOUS_ICHI=True; "
                f"ICHI_BUY={ichi_buy}; ICHI_SELL={ichi_sell}"
            ),
        }

    buy_window = _signal_time_window_minutes_for_side(
        adx_analyze_result_obj,
        mcad_analyze_result_obj,
        ichimoku_result_k,
        ichimoku_result_s,
        "BUY",
    )
    sell_window = _signal_time_window_minutes_for_side(
        adx_analyze_result_obj,
        mcad_analyze_result_obj,
        ichimoku_result_k,
        ichimoku_result_s,
        "SELL",
    )

    base_conditions = (
        f"PERIOD={period}; LIMITS={limits}; ADX_BUY={adx_buy}; ADX_SELL={adx_sell}; "
        f"ADX_INC={adx_inc}; ADX_DEC={adx_dec}; MCAD_BUY={mcad_buy}; MCAD_SELL={mcad_sell}; "
        f"MCAD_FRESH={mcad_fresh}; ICHI_BUY={ichi_buy}; ICHI_SELL={ichi_sell}; "
        f"ICHI_PRICE_VS_CLOUD={ichimoku_price_vs_cloud}; "
        f"ICHI_BUY_STRONG={ichi_buy_strong}; ICHI_BUY_WEAK={ichi_buy_weak}; "
        f"ICHI_SELL_STRONG={ichi_sell_strong}; ICHI_SELL_WEAK={ichi_sell_weak}; "
        f"ICHI_TK_BUY={ichi_tk_buy}; ICHI_PRICE_KIJUN_BUY={ichi_price_kijun_buy}; ICHI_CLOUD_BREAKOUT_BUY={ichi_cloud_breakout_buy}; "
        f"ICHI_TK_SELL={ichi_tk_sell}; ICHI_PRICE_KIJUN_SELL={ichi_price_kijun_sell}; ICHI_CLOUD_BREAKOUT_SELL={ichi_cloud_breakout_sell}; "
        f"BUY_WINDOW_MIN={buy_window}; SELL_WINDOW_MIN={sell_window}; "
        f"CANDLE_SIGNAL={candle_signal}; CANDLE_PATTERNS={candle_patterns}; "
        f"CANDLE_BUY={candle_buy}; CANDLE_SELL={candle_sell}"
    )

    # SC9/SC16: Ichimoku trend + ADX strength + MCAD momentum + candle trigger.
    if ichi_buy and adx_inc and mcad_buy and candle_buy and not ichi_sell and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(buy_window, limits["strict"]):
        return {
            "signal": "BUY",
            "scenario_number": 9,
            "scenario_conditions": f"SC9 BUY (ICHI trend + ADX strength + MCAD + candle trigger, strict) | {base_conditions}",
        }

    if ichi_sell and adx_inc and mcad_sell and candle_sell and not ichi_buy and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(sell_window, limits["strict"]):
        return {
            "signal": "SELL",
            "scenario_number": 16,
            "scenario_conditions": f"SC16 SELL (ICHI trend + ADX strength + MCAD + candle trigger, strict) | {base_conditions}",
        }

    # SC10/SC17: Ichimoku breakout + ADX DI cross + MCAD cross + candle confirmation.
    if ichi_buy and adx_buy and mcad_buy and candle_buy and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(buy_window, limits["medium"]):
        return {
            "signal": "BUY",
            "scenario_number": 10,
            "scenario_conditions": f"SC10 BUY (ICHI breakout + ADX cross + MCAD cross + candle confirmation, medium) | {base_conditions}",
        }

    if ichi_sell and adx_sell and mcad_sell and candle_sell and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(sell_window, limits["medium"]):
        return {
            "signal": "SELL",
            "scenario_number": 17,
            "scenario_conditions": f"SC17 SELL (ICHI breakout + ADX cross + MCAD cross + candle confirmation, medium) | {base_conditions}",
        }

    # SC11/SC18: Ichimoku trend filter + ADX strength + no opposite MCAD + candle trigger.
    if ichi_buy and adx_buy and adx_inc and not mcad_sell and candle_buy and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(buy_window, limits["medium"]):
        return {
            "signal": "BUY",
            "scenario_number": 11,
            "scenario_conditions": f"SC11 BUY (ICHI trend filter + ADX strength + MACD filter + candle trigger, medium) | {base_conditions}",
        }

    if ichi_sell and adx_sell and adx_inc and not mcad_buy and candle_sell and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(sell_window, limits["medium"]):
        return {
            "signal": "SELL",
            "scenario_number": 18,
            "scenario_conditions": f"SC18 SELL (ICHI trend filter + ADX strength + MACD filter + candle trigger, medium) | {base_conditions}",
        }

    # SC12/SC19: Ichimoku breakout + fresh MCAD momentum + candle trigger.
    if ichi_buy and mcad_buy and mcad_fresh and candle_buy and not adx_sell and _within_window(buy_window, limits["relaxed"]):
        return {
            "signal": "BUY",
            "scenario_number": 12,
            "scenario_conditions": f"SC12 BUY (ICHI breakout + fresh MCAD momentum + candle trigger, relaxed) | {base_conditions}",
        }

    if ichi_sell and mcad_sell and mcad_fresh and candle_sell and not adx_buy and _within_window(sell_window, limits["relaxed"]):
        return {
            "signal": "SELL",
            "scenario_number": 19,
            "scenario_conditions": f"SC19 SELL (ICHI breakout + fresh MCAD momentum + candle trigger, relaxed) | {base_conditions}",
        }

    # SC13/SC20: Candle-led trigger with Ichimoku trend and ADX strength filter.
    if candle_buy and ichi_buy and adx_inc and not mcad_sell and _within_window(buy_window, limits["relaxed"]):
        return {
            "signal": "BUY",
            "scenario_number": 13,
            "scenario_conditions": f"SC13 BUY (candle trigger + ICHI trend + ADX strength, relaxed) | {base_conditions}",
        }

    if candle_sell and ichi_sell and adx_inc and not mcad_buy and _within_window(sell_window, limits["relaxed"]):
        return {
            "signal": "SELL",
            "scenario_number": 20,
            "scenario_conditions": f"SC20 SELL (candle trigger + ICHI trend + ADX strength, relaxed) | {base_conditions}",
        }

    # SC14/SC21: Ichimoku as trend filter, ADX as strength filter, MACD momentum + candle confirmation.
    if ichi_buy and adx_inc and mcad_buy and candle_buy and not adx_sell and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(buy_window, limits["medium"]):
        return {
            "signal": "BUY",
            "scenario_number": 14,
            "scenario_conditions": f"SC14 BUY (ICHI filter + ADX strength + MCAD momentum + candle confirmation, medium) | {base_conditions}",
        }

    if ichi_sell and adx_inc and mcad_sell and candle_sell and not adx_buy and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(sell_window, limits["medium"]):
        return {
            "signal": "SELL",
            "scenario_number": 21,
            "scenario_conditions": f"SC21 SELL (ICHI filter + ADX strength + MCAD momentum + candle confirmation, medium) | {base_conditions}",
        }

    # SC15/SC22: Candle confirmation + ADX DI cross + MCAD DI direction, Ichimoku as opposite-side blocker.
    if candle_buy and adx_buy and mcad_buy and not ichi_sell and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(buy_window, limits["strict"]):
        return {
            "signal": "BUY",
            "scenario_number": 15,
            "scenario_conditions": f"SC15 BUY (candle confirmation + ADX cross + MCAD direction, strict) | {base_conditions}",
        }

    if candle_sell and adx_sell and mcad_sell and not ichi_buy and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(sell_window, limits["strict"]):
        return {
            "signal": "SELL",
            "scenario_number": 22,
            "scenario_conditions": f"SC22 SELL (candle confirmation + ADX cross + MCAD direction, strict) | {base_conditions}",
        }

    # SC1 (BUY): ADX(DI cross) + MCAD with ADX momentum and fresh MCAD cross.
    # WYŁĄCZONY: Win Rate 11.1%, strata -23.88 EUR (9 transakcji)
    # if adx_buy and adx_inc and mcad_buy and mcad_fresh and not ichi_sell and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(buy_window, limits["strict"]):
    #     return {
    #         "signal": "BUY",
    #         "scenario_number": 1,
    #         "scenario_conditions": f"SC1 BUY (ADX+MCAD, strict) | {base_conditions}",
    #     }

    # SC2 (BUY): Ichimoku + ADX.
    if adx_buy and ichi_buy and not mcad_sell and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(buy_window, limits["medium"]):
        return {
            "signal": "BUY",
            "scenario_number": 2,
            "scenario_conditions": f"SC2 BUY (ICHI+ADX, medium) | {base_conditions}",
        }

    # SC3 (BUY): Ichimoku + MCAD.
    if mcad_buy and ichi_buy and not adx_sell and _within_window(buy_window, limits["relaxed"]):
        return {
            "signal": "BUY",
            "scenario_number": 3,
            "scenario_conditions": f"SC3 BUY (ICHI+MCAD, relaxed) | {base_conditions}",
        }

    # SC4 (BUY): Tenkan/Kijun cross nad chmura (silny sygnal), bez przeciwnych sygnalow ADX/MCAD.
    if ichi_tk_buy and ichi_buy_strong and not adx_sell and not mcad_sell and _within_window(buy_window, limits["relaxed"]):
        return {
            "signal": "BUY",
            "scenario_number": 4,
            "scenario_conditions": f"SC4 BUY (ICHI TK/Kijun cross, strong above cloud, relaxed) | {base_conditions}",
        }

    # SC23 (BUY): Tenkan/Kijun cross pod/w chmurze (slaby sygnal), bez przeciwnych sygnalow ADX/MCAD.
    if ichi_tk_buy and ichi_buy_weak and not adx_sell and not mcad_sell and _within_window(buy_window, limits["relaxed"]):
        return {
            "signal": "BUY",
            "scenario_number": 23,
            "scenario_conditions": f"SC23 BUY (ICHI TK/Kijun cross, weak below/inside cloud, relaxed) | {base_conditions}",
        }

    # SC24 (BUY): Price/Kijun cross nad chmura (silny sygnal), bez przeciwnych sygnalow ADX/MCAD.
    if ichi_price_kijun_buy and ichi_buy_strong and not adx_sell and not mcad_sell and _within_window(buy_window, limits["relaxed"]):
        return {
            "signal": "BUY",
            "scenario_number": 24,
            "scenario_conditions": f"SC24 BUY (ICHI Price/Kijun cross, strong above cloud, relaxed) | {base_conditions}",
        }

    # SC25 (BUY): Price/Kijun cross pod/w chmurze (slaby sygnal), bez przeciwnych sygnalow ADX/MCAD.
    if ichi_price_kijun_buy and ichi_buy_weak and not adx_sell and not mcad_sell and _within_window(buy_window, limits["relaxed"]):
        return {
            "signal": "BUY",
            "scenario_number": 25,
            "scenario_conditions": f"SC25 BUY (ICHI Price/Kijun cross, weak below/inside cloud, relaxed) | {base_conditions}",
        }

    # SC26 (BUY): wybicie ceny z chmury w gore, bez przeciwnych sygnalow ADX/MCAD.
    if ichi_cloud_breakout_buy and not adx_sell and not mcad_sell and _within_window(buy_window, limits["relaxed"]):
        return {
            "signal": "BUY",
            "scenario_number": 26,
            "scenario_conditions": f"SC26 BUY (ICHI cloud breakout up, relaxed) | {base_conditions}",
        }

    # SC27 (BUY): fallback Ichimoku-only, gdy sygnal BUY istnieje, ale nie pasuje do podtypow.
    if ichi_buy and not adx_sell and not mcad_sell and _within_window(buy_window, limits["relaxed"]):
        return {
            "signal": "BUY",
            "scenario_number": 27,
            "scenario_conditions": f"SC27 BUY (ICHI only fallback, relaxed) | {base_conditions}",
        }

    # SC5 (SELL): ADX(DI cross) + MCAD with ADX momentum and fresh MCAD cross.
    if adx_sell and adx_inc and mcad_sell and mcad_fresh and not ichi_buy and ichi_sell_strong and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(sell_window, limits["strict"]):
        return {
            "signal": "SELL",
            "scenario_number": 5,
            "scenario_conditions": f"SC5 SELL (ADX+MCAD, strict, strong below cloud) | {base_conditions}",
        }

    if adx_sell and adx_inc and mcad_sell and mcad_fresh and not ichi_buy and ichi_sell_weak and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(sell_window, limits["strict"]):
        return {
            "signal": "SELL",
            "scenario_number": 5,
            "scenario_conditions": f"SC5 SELL (ADX+MCAD, strict, weak above/inside cloud) | {base_conditions}",
        }

    if adx_sell and adx_inc and mcad_sell and mcad_fresh and not ichi_buy and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(sell_window, limits["strict"]):
        return {
            "signal": "SELL",
            "scenario_number": 5,
            "scenario_conditions": f"SC5 SELL (ADX+MCAD, strict, cloud unknown) | {base_conditions}",
        }

    # SC6 (SELL): Ichimoku + ADX.
    if adx_sell and ichi_sell and ichi_sell_strong and not mcad_buy and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(sell_window, limits["medium"]):
        return {
            "signal": "SELL",
            "scenario_number": 6,
            "scenario_conditions": f"SC6 SELL (ICHI+ADX, medium, strong below cloud) | {base_conditions}",
        }

    if adx_sell and ichi_sell and ichi_sell_weak and not mcad_buy and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(sell_window, limits["medium"]):
        return {
            "signal": "SELL",
            "scenario_number": 6,
            "scenario_conditions": f"SC6 SELL (ICHI+ADX, medium, weak above/inside cloud) | {base_conditions}",
        }

    if adx_sell and ichi_sell and not mcad_buy and not _is_price_inside_cloud(ichimoku_price_vs_cloud) and _within_window(sell_window, limits["medium"]):
        return {
            "signal": "SELL",
            "scenario_number": 6,
            "scenario_conditions": f"SC6 SELL (ICHI+ADX, medium, cloud unknown) | {base_conditions}",
        }

    # SC7 (SELL): Ichimoku + MCAD.
    if mcad_sell and ichi_sell and ichi_sell_strong and not adx_buy and _within_window(sell_window, limits["relaxed"]):
        return {
            "signal": "SELL",
            "scenario_number": 7,
            "scenario_conditions": f"SC7 SELL (ICHI+MCAD, relaxed, strong below cloud) | {base_conditions}",
        }

    if mcad_sell and ichi_sell and ichi_sell_weak and not adx_buy and _within_window(sell_window, limits["relaxed"]):
        return {
            "signal": "SELL",
            "scenario_number": 7,
            "scenario_conditions": f"SC7 SELL (ICHI+MCAD, relaxed, weak above/inside cloud) | {base_conditions}",
        }

    if mcad_sell and ichi_sell and not adx_buy and _within_window(sell_window, limits["relaxed"]):
        return {
            "signal": "SELL",
            "scenario_number": 7,
            "scenario_conditions": f"SC7 SELL (ICHI+MCAD, relaxed, cloud unknown) | {base_conditions}",
        }

    # SC28 (SELL): Tenkan/Kijun cross pod chmura (silny sygnal), bez przeciwnych sygnalow ADX/MCAD.
    if ichi_tk_sell and ichi_sell_strong and not adx_buy and not mcad_buy and _within_window(sell_window, limits["relaxed"]):
        return {
            "signal": "SELL",
            "scenario_number": 28,
            "scenario_conditions": f"SC28 SELL (ICHI TK/Kijun cross, strong below cloud, relaxed) | {base_conditions}",
        }

    # SC29 (SELL): Tenkan/Kijun cross nad/w chmurze (slaby sygnal), bez przeciwnych sygnalow ADX/MCAD.
    if ichi_tk_sell and ichi_sell_weak and not adx_buy and not mcad_buy and _within_window(sell_window, limits["relaxed"]):
        return {
            "signal": "SELL",
            "scenario_number": 29,
            "scenario_conditions": f"SC29 SELL (ICHI TK/Kijun cross, weak above/inside cloud, relaxed) | {base_conditions}",
        }

    # SC30 (SELL): Price/Kijun cross pod chmura (silny sygnal), bez przeciwnych sygnalow ADX/MCAD.
    if ichi_price_kijun_sell and ichi_sell_strong and not adx_buy and not mcad_buy and _within_window(sell_window, limits["relaxed"]):
        return {
            "signal": "SELL",
            "scenario_number": 30,
            "scenario_conditions": f"SC30 SELL (ICHI Price/Kijun cross, strong below cloud, relaxed) | {base_conditions}",
        }

    # SC31 (SELL): Price/Kijun cross nad/w chmurze (slaby sygnal), bez przeciwnych sygnalow ADX/MCAD.
    if ichi_price_kijun_sell and ichi_sell_weak and not adx_buy and not mcad_buy and _within_window(sell_window, limits["relaxed"]):
        return {
            "signal": "SELL",
            "scenario_number": 31,
            "scenario_conditions": f"SC31 SELL (ICHI Price/Kijun cross, weak above/inside cloud, relaxed) | {base_conditions}",
        }

    # SC32 (SELL): wybicie ceny z chmury w dol, bez przeciwnych sygnalow ADX/MCAD.
    if ichi_cloud_breakout_sell and not adx_buy and not mcad_buy and _within_window(sell_window, limits["relaxed"]):
        return {
            "signal": "SELL",
            "scenario_number": 32,
            "scenario_conditions": f"SC32 SELL (ICHI cloud breakout down, relaxed) | {base_conditions}",
        }

    # SC8 (SELL): tylko Ichimoku, bez przeciwnych sygnalow ADX/MCAD.
    if ichi_sell and ichi_sell_strong and not adx_buy and not mcad_buy and _within_window(sell_window, limits["relaxed"]):
        return {
            "signal": "SELL",
            "scenario_number": 8,
            "scenario_conditions": f"SC8 SELL (ICHI only, relaxed, strong below cloud) | {base_conditions}",
        }

    if ichi_sell and ichi_sell_weak and not adx_buy and not mcad_buy and _within_window(sell_window, limits["relaxed"]):
        return {
            "signal": "SELL",
            "scenario_number": 8,
            "scenario_conditions": f"SC8 SELL (ICHI only, relaxed, weak above/inside cloud) | {base_conditions}",
        }

    if ichi_sell and not adx_buy and not mcad_buy and _within_window(sell_window, limits["relaxed"]):
        return {
            "signal": "SELL",
            "scenario_number": 8,
            "scenario_conditions": f"SC8 SELL (ICHI only, relaxed, cloud unknown) | {base_conditions}",
        }

    return {
        "signal": None,
        "scenario_number": None,
        "scenario_conditions": f"NO_MATCH | {base_conditions}",
    }


def get_buy_signal(
    adx_analyze_result_obj: tuple[Any, Any],
    mcad_analyze_result_obj: Any,
    ichimoku_result_k: list[str],
    ichimoku_result_s: list[str],
    ichimoku_price_vs_cloud: str | None = None,
    max_time_result_minutes: int | None = None,
) -> dict[str, Any]:
    # Backward-compatible wrapper for old call sites expecting BUY-only semantics.
    result = get_trade_signal(
        adx_analyze_result_obj,
        mcad_analyze_result_obj,
        ichimoku_result_k,
        ichimoku_result_s,
        ichimoku_price_vs_cloud,
        period=None,
        max_time_result_minutes=max_time_result_minutes,
        candle_pattern_signal=None,
    )
    if result.get("signal") == "BUY":
        return result
    return {
        "signal": None,
        "scenario_number": None,
        "scenario_conditions": result.get("scenario_conditions"),
    }
