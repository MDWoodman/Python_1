from __future__ import annotations

import re
from typing import Any

from wskazniki import adx__chat as adxcht
from wskazniki import mcad__chat as mcad


def _has_ichi_buy_signal(ichimoku_result_k: list[str]) -> bool:
    if not ichimoku_result_k:
        return False

    keywords = [
        "Przeciecie_do_gory",
        "price_senokuspan_result_enum.Przeciecie_do_gory",
        "price_kiusen_result_enum.Przeciecie_do_gory",
        "tenkansen_kiusen_result_enum.Przeciecie_do_gory",
    ]
    return any(any(keyword in entry for keyword in keywords) for entry in ichimoku_result_k)


def _has_ichi_sell_signal(ichimoku_result_s: list[str]) -> bool:
    if not ichimoku_result_s:
        return False

    keywords = [
        "Przeciecie_do_dolu",
        "price_senokuspan_result_enum.Przeciecie_do_dolu",
        "price_kiusen_result_enum.Przeciecie_do_dolu",
        "tenkansen_kiusen_result_enum.Przeciecie_do_dolu",
    ]
    return any(any(keyword in entry for keyword in keywords) for entry in ichimoku_result_s)


def _extract_ichi_times(entries: list[str]) -> list[int]:
    times: list[int] = []
    for entry in entries:
        matches = re.findall(r"\d{10,13}", entry)
        if not matches:
            continue
        try:
            times.append(int(matches[0]))
        except ValueError:
            continue
    return times


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


def _mcad_is_buy(mcad_analyze_result_obj: Any) -> bool:
    return (
        mcad_analyze_result_obj is not None
        and mcad_analyze_result_obj.get_result() == mcad.mcad_result_enum.Wzrost_przeciecie
    )


def _mcad_is_sell(mcad_analyze_result_obj: Any) -> bool:
    return (
        mcad_analyze_result_obj is not None
        and mcad_analyze_result_obj.get_result() == mcad.mcad_result_enum.Spadek_przeciecie
    )


def _close_time_limits(period: str | None, max_time_result_minutes: int | None) -> dict[str, int]:
    period_upper = str(period or "").upper()

    # Wider confirmation windows for H1/H4 close decisions.
    if period_upper == "H1":
        return {"strict": 240, "medium": 480, "relaxed": 960}
    if period_upper == "H4":
        return {"strict": 960, "medium": 1920, "relaxed": 3840}

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


def _close_window_minutes(
    adx_analyze_result_obj: tuple[Any, Any],
    mcad_analyze_result_obj: Any,
    ichimoku_result_k: list[str],
    ichimoku_result_s: list[str],
    transaction_type: str,
) -> float | None:
    side = str(transaction_type or "").upper()
    timestamps: list[int] = []

    # Close BUY when SELL indicators align.
    if side == "BUY":
        if _adx_is_sell(adx_analyze_result_obj):
            adx_time = adx_analyze_result_obj[0].get_time()
            if adx_time is not None:
                timestamps.append(int(adx_time))

        if _mcad_is_sell(mcad_analyze_result_obj):
            mcad_time = mcad_analyze_result_obj.get_time()
            if mcad_time is not None:
                timestamps.append(int(mcad_time))

        ichi_times = _extract_ichi_times(ichimoku_result_s)
        if ichi_times:
            timestamps.append(max(ichi_times))

    # Close SELL when BUY indicators align.
    if side == "SELL":
        if _adx_is_buy(adx_analyze_result_obj):
            adx_time = adx_analyze_result_obj[0].get_time()
            if adx_time is not None:
                timestamps.append(int(adx_time))

        if _mcad_is_buy(mcad_analyze_result_obj):
            mcad_time = mcad_analyze_result_obj.get_time()
            if mcad_time is not None:
                timestamps.append(int(mcad_time))

        ichi_times = _extract_ichi_times(ichimoku_result_k)
        if ichi_times:
            timestamps.append(max(ichi_times))

    if len(timestamps) < 2:
        return None

    return (max(timestamps) - min(timestamps)) / 60000.0


def get_close_signal(
    opened_transaction_type: str,
    adx_analyze_result_obj: tuple[Any, Any],
    mcad_analyze_result_obj: Any,
    ichimoku_result_k: list[str],
    ichimoku_result_s: list[str],
    period: str | None,
    max_time_result_minutes: int | None = None,
) -> dict[str, Any]:
    tx_type = str(opened_transaction_type or "").upper()
    limits = _close_time_limits(period, max_time_result_minutes)

    adx_buy = _adx_is_buy(adx_analyze_result_obj)
    adx_sell = _adx_is_sell(adx_analyze_result_obj)
    mcad_buy = _mcad_is_buy(mcad_analyze_result_obj)
    mcad_sell = _mcad_is_sell(mcad_analyze_result_obj)
    ichi_buy = _has_ichi_buy_signal(ichimoku_result_k)
    ichi_sell = _has_ichi_sell_signal(ichimoku_result_s)

    window = _close_window_minutes(
        adx_analyze_result_obj,
        mcad_analyze_result_obj,
        ichimoku_result_k,
        ichimoku_result_s,
        tx_type,
    )

    base = (
        f"TX={tx_type}; PERIOD={period}; LIMITS={limits}; WINDOW_MIN={window}; "
        f"ADX_BUY={adx_buy}; ADX_SELL={adx_sell}; MCAD_BUY={mcad_buy}; MCAD_SELL={mcad_sell}; "
        f"ICHI_BUY={ichi_buy}; ICHI_SELL={ichi_sell}"
    )

    if tx_type == "BUY":
        # C1-C3: closing BUY with SELL evidence.
        if adx_sell and mcad_sell and ichi_sell and _within_window(window, limits["strict"]):
            return {"close": True, "scenario_number": 1, "scenario_conditions": f"C1 CLOSE BUY strong | {base}"}
        if adx_sell and ichi_sell and not mcad_buy and _within_window(window, limits["medium"]):
            return {"close": True, "scenario_number": 2, "scenario_conditions": f"C2 CLOSE BUY trend | {base}"}
        if mcad_sell and ichi_sell and not adx_buy and _within_window(window, limits["relaxed"]):
            return {"close": True, "scenario_number": 3, "scenario_conditions": f"C3 CLOSE BUY momentum | {base}"}

    if tx_type == "SELL":
        # C4-C6: closing SELL with BUY evidence.
        if adx_buy and mcad_buy and ichi_buy and _within_window(window, limits["strict"]):
            return {"close": True, "scenario_number": 4, "scenario_conditions": f"C4 CLOSE SELL strong | {base}"}
        if adx_buy and ichi_buy and not mcad_sell and _within_window(window, limits["medium"]):
            return {"close": True, "scenario_number": 5, "scenario_conditions": f"C5 CLOSE SELL trend | {base}"}
        if mcad_buy and ichi_buy and not adx_sell and _within_window(window, limits["relaxed"]):
            return {"close": True, "scenario_number": 6, "scenario_conditions": f"C6 CLOSE SELL momentum | {base}"}

    return {"close": False, "scenario_number": None, "scenario_conditions": f"NO_CLOSE | {base}"}
