"""Risk management helpers for SL/TP logic in trading strategies.

The module is designed for pandas DataFrame market data and supports
multiple Stop Loss and Take Profit algorithms.
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd


Direction = str

BASE_REQUIRED_COLUMNS: set[str] = {"open", "high", "low", "close", "tick_volume"}


def _validate_dataframe(df: pd.DataFrame) -> None:
    """Validate that input is a non-empty DataFrame."""
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")
    if df.empty:
        raise ValueError("df must not be empty")


def _validate_columns(df: pd.DataFrame, required_columns: Iterable[str]) -> None:
    """Validate that required columns exist in DataFrame."""
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _validate_direction(direction: str) -> None:
    """Validate position direction."""
    if direction not in {"long", "short"}:
        raise ValueError("direction must be 'long' or 'short'")


def _validate_index(df: pd.DataFrame, index_value: int, field_name: str) -> None:
    """Validate integer index boundaries for DataFrame row access."""
    if not isinstance(index_value, int):
        raise TypeError(f"{field_name} must be int")
    if index_value < 0 or index_value >= len(df):
        raise IndexError(f"{field_name} out of range: {index_value}")


def _validate_positive(value: float, field_name: str, allow_zero: bool = False) -> None:
    """Validate numeric parameter positivity."""
    if not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be int or float")
    if allow_zero:
        if value < 0:
            raise ValueError(f"{field_name} must be >= 0")
    else:
        if value <= 0:
            raise ValueError(f"{field_name} must be > 0")


def _validate_lookback(lookback: int) -> None:
    """Validate lookback window size."""
    if not isinstance(lookback, int):
        raise TypeError("lookback must be int")
    if lookback < 1:
        raise ValueError("lookback must be >= 1")


def _get_window_start(entry_index: int, lookback: int) -> int:
    """Return start index for lookback window."""
    return max(0, entry_index - lookback + 1)


def _local_extrema_indices(series: pd.Series, mode: str) -> list[int]:
    """Find simple local extrema indices inside a Series.

    mode='min' finds local minima, mode='max' finds local maxima.
    """
    if mode not in {"min", "max"}:
        raise ValueError("mode must be 'min' or 'max'")

    extrema_indices: list[int] = []
    values = series.reset_index(drop=True)
    if len(values) < 3:
        return extrema_indices

    for i in range(1, len(values) - 1):
        prev_val = float(values.iloc[i - 1])
        curr_val = float(values.iloc[i])
        next_val = float(values.iloc[i + 1])
        if mode == "min" and curr_val <= prev_val and curr_val <= next_val:
            extrema_indices.append(i)
        if mode == "max" and curr_val >= prev_val and curr_val >= next_val:
            extrema_indices.append(i)

    return extrema_indices


def _prepare_common(df: pd.DataFrame, entry_index: int, direction: Direction) -> None:
    """Run shared validation for core SL/TP calculations."""
    _validate_dataframe(df)
    _validate_columns(df, BASE_REQUIRED_COLUMNS)
    _validate_index(df, entry_index, "entry_index")
    _validate_direction(direction)


def calculate_sl_swing(
    df: pd.DataFrame,
    entry_index: int,
    direction: Direction,
    buffer: float,
    lookback: int = 10,
) -> float:
    """Set SL by recent swing low/high from lookback window."""
    _prepare_common(df, entry_index, direction)
    _validate_positive(buffer, "buffer", allow_zero=True)
    _validate_lookback(lookback)

    start = _get_window_start(entry_index, lookback)
    window = df.iloc[start : entry_index + 1]
    if len(window) < 2:
        raise ValueError("Not enough candles to calculate swing SL")

    if direction == "long":
        swing_low = float(window["low"].min())
        return swing_low - buffer

    swing_high = float(window["high"].max())
    return swing_high + buffer


def calculate_sl_kijun(
    df: pd.DataFrame,
    entry_index: int,
    direction: Direction,
    buffer: float,
) -> float:
    """Set SL relative to Kijun level."""
    _prepare_common(df, entry_index, direction)
    _validate_columns(df, {"kijun"})
    _validate_positive(buffer, "buffer", allow_zero=True)

    kijun = float(df.iloc[entry_index]["kijun"])
    if direction == "long":
        return kijun - buffer
    return kijun + buffer


def calculate_sl_cloud(
    df: pd.DataFrame,
    entry_index: int,
    direction: Direction,
    buffer: float,
) -> float:
    """Set SL using Ichimoku cloud bounds."""
    _prepare_common(df, entry_index, direction)
    _validate_columns(df, {"span_a", "span_b"})
    _validate_positive(buffer, "buffer", allow_zero=True)

    span_a = float(df.iloc[entry_index]["span_a"])
    span_b = float(df.iloc[entry_index]["span_b"])
    cloud_low = min(span_a, span_b)
    cloud_high = max(span_a, span_b)

    if direction == "long":
        return cloud_low - buffer
    return cloud_high + buffer


def calculate_sl_atr(
    df: pd.DataFrame,
    entry_index: int,
    entry_price: float,
    direction: Direction,
    atr_multiplier: float,
) -> float:
    """Set SL as ATR-based distance from entry price."""
    _prepare_common(df, entry_index, direction)
    _validate_columns(df, {"atr"})
    _validate_positive(entry_price, "entry_price")
    _validate_positive(atr_multiplier, "atr_multiplier")

    atr_value = float(df.iloc[entry_index]["atr"])
    _validate_positive(atr_value, "atr_value")

    if direction == "long":
        return entry_price - atr_value * atr_multiplier
    return entry_price + atr_value * atr_multiplier


def calculate_sl_signal_candle(
    df: pd.DataFrame,
    entry_index: int,
    direction: Direction,
    buffer: float,
) -> float:
    """Set SL under/over signal candle low/high."""
    _prepare_common(df, entry_index, direction)
    _validate_positive(buffer, "buffer", allow_zero=True)

    signal_row = df.iloc[entry_index]
    if direction == "long":
        return float(signal_row["low"]) - buffer
    return float(signal_row["high"]) + buffer


def calculate_sl_support_resistance(
    df: pd.DataFrame,
    entry_index: int,
    direction: Direction,
    lookback: int,
    buffer: float,
) -> float:
    """Set SL using nearest local support/resistance in lookback window."""
    _prepare_common(df, entry_index, direction)
    _validate_lookback(lookback)
    _validate_positive(buffer, "buffer", allow_zero=True)

    start = _get_window_start(entry_index, lookback)
    window = df.iloc[start : entry_index + 1]
    if len(window) < 3:
        raise ValueError("Not enough candles to detect local support/resistance")

    current_price = float(df.iloc[entry_index]["close"])

    if direction == "long":
        local_lows_idx = _local_extrema_indices(window["low"], mode="min")
        support_candidates = [
            float(window["low"].iloc[i])
            for i in local_lows_idx
            if float(window["low"].iloc[i]) < current_price
        ]
        if not support_candidates:
            support_level = float(window["low"].min())
        else:
            support_level = max(support_candidates)
        return support_level - buffer

    local_highs_idx = _local_extrema_indices(window["high"], mode="max")
    resistance_candidates = [
        float(window["high"].iloc[i])
        for i in local_highs_idx
        if float(window["high"].iloc[i]) > current_price
    ]
    if not resistance_candidates:
        resistance_level = float(window["high"].max())
    else:
        resistance_level = min(resistance_candidates)
    return resistance_level + buffer


def check_exit_condition(
    df: pd.DataFrame,
    entry_index: int,
    current_index: int,
    direction: Direction,
    max_bars: int,
    adx_threshold: float,
) -> bool:
    """Return True if conditional exit is required."""
    _prepare_common(df, entry_index, direction)
    _validate_index(df, current_index, "current_index")
    _validate_positive(max_bars, "max_bars")
    _validate_positive(adx_threshold, "adx_threshold", allow_zero=True)
    _validate_columns(df, {"adx"})

    if current_index < entry_index:
        raise ValueError("current_index must be >= entry_index")

    bars_in_trade = current_index - entry_index
    current_adx = float(df.iloc[current_index]["adx"])
    if current_adx < adx_threshold:
        return True

    if bars_in_trade > max_bars:
        entry_close = float(df.iloc[entry_index]["close"])
        current_close = float(df.iloc[current_index]["close"])
        if direction == "long" and current_close <= entry_close:
            return True
        if direction == "short" and current_close >= entry_close:
            return True

    return False


def calculate_tp_rr(
    entry_price: float,
    sl_price: float,
    direction: Direction,
    rr_ratio: float,
) -> float:
    """Calculate TP from Risk:Reward ratio."""
    _validate_direction(direction)
    _validate_positive(entry_price, "entry_price")
    _validate_positive(sl_price, "sl_price")
    _validate_positive(rr_ratio, "rr_ratio")

    if direction == "long":
        risk = entry_price - sl_price
        if risk <= 0:
            raise ValueError("For long, sl_price must be below entry_price")
        return entry_price + risk * rr_ratio

    risk = sl_price - entry_price
    if risk <= 0:
        raise ValueError("For short, sl_price must be above entry_price")
    return entry_price - risk * rr_ratio


def calculate_tp_support_resistance(
    df: pd.DataFrame,
    entry_index: int,
    direction: Direction,
    lookback: int,
) -> float:
    """Calculate TP from nearest local resistance/support."""
    _prepare_common(df, entry_index, direction)
    _validate_lookback(lookback)

    start = _get_window_start(entry_index, lookback)
    window = df.iloc[start : entry_index + 1]
    if len(window) < 3:
        raise ValueError("Not enough candles to detect TP support/resistance")

    current_price = float(df.iloc[entry_index]["close"])

    if direction == "long":
        local_highs_idx = _local_extrema_indices(window["high"], mode="max")
        resistance_candidates = [
            float(window["high"].iloc[i])
            for i in local_highs_idx
            if float(window["high"].iloc[i]) > current_price
        ]
        if not resistance_candidates:
            return float(window["high"].max())
        return min(resistance_candidates)

    local_lows_idx = _local_extrema_indices(window["low"], mode="min")
    support_candidates = [
        float(window["low"].iloc[i])
        for i in local_lows_idx
        if float(window["low"].iloc[i]) < current_price
    ]
    if not support_candidates:
        return float(window["low"].min())
    return max(support_candidates)


def calculate_tp_atr(
    df: pd.DataFrame,
    entry_index: int,
    entry_price: float,
    direction: Direction,
    multiplier: float,
) -> float:
    """Calculate TP as ATR-based distance from entry."""
    _prepare_common(df, entry_index, direction)
    _validate_columns(df, {"atr"})
    _validate_positive(entry_price, "entry_price")
    _validate_positive(multiplier, "multiplier")

    atr_value = float(df.iloc[entry_index]["atr"])
    _validate_positive(atr_value, "atr_value")

    if direction == "long":
        return entry_price + atr_value * multiplier
    return entry_price - atr_value * multiplier


def calculate_tp_ichimoku(df: pd.DataFrame, entry_index: int, direction: Direction) -> float:
    """Calculate TP from Ichimoku levels.

    For long, target nearest sensible level above current close.
    For short, target nearest sensible level below current close.
    """
    _prepare_common(df, entry_index, direction)
    _validate_columns(df, {"kijun", "span_a", "span_b"})

    row = df.iloc[entry_index]
    current_close = float(row["close"])
    levels = sorted({float(row["kijun"]), float(row["span_a"]), float(row["span_b"])})

    if direction == "long":
        above = [level for level in levels if level > current_close]
        if above:
            return min(above)
        return current_close + abs(current_close - levels[-1])

    below = [level for level in levels if level < current_close]
    if below:
        return max(below)
    return current_close - abs(levels[0] - current_close)


def calculate_partial_tp(
    entry_price: float,
    sl_price: float,
    direction: Direction,
) -> tuple[float, float]:
    """Return two partial TP levels: RR 1:1 and RR 1:2."""
    tp1 = calculate_tp_rr(entry_price, sl_price, direction, rr_ratio=1.0)
    tp2 = calculate_tp_rr(entry_price, sl_price, direction, rr_ratio=2.0)
    return tp1, tp2


def update_trailing_sl_kijun(
    df: pd.DataFrame,
    current_index: int,
    direction: Direction,
    current_sl: float,
) -> float:
    """Update trailing SL using Kijun, without loosening risk."""
    _validate_dataframe(df)
    _validate_columns(df, BASE_REQUIRED_COLUMNS | {"kijun"})
    _validate_index(df, current_index, "current_index")
    _validate_direction(direction)
    _validate_positive(current_sl, "current_sl")

    kijun_value = float(df.iloc[current_index]["kijun"])
    if direction == "long":
        return max(current_sl, kijun_value)
    return min(current_sl, kijun_value)


def check_tp_exit(df: pd.DataFrame, current_index: int, direction: Direction) -> bool:
    """Return True if dynamic TP exit condition is met.

    Conditions include MACD reversal, histogram weakening, and ADX decline.
    """
    _validate_dataframe(df)
    _validate_columns(df, BASE_REQUIRED_COLUMNS | {"macd", "signal", "histogram", "adx"})
    _validate_index(df, current_index, "current_index")
    _validate_direction(direction)

    if current_index < 1:
        return False

    current_row = df.iloc[current_index]
    prev_row = df.iloc[current_index - 1]

    macd_curr = float(current_row["macd"])
    signal_curr = float(current_row["signal"])
    hist_curr = float(current_row["histogram"])
    hist_prev = float(prev_row["histogram"])
    adx_curr = float(current_row["adx"])
    adx_prev = float(prev_row["adx"])

    adx_declining = adx_curr < adx_prev

    if direction == "long":
        macd_reversal = macd_curr < signal_curr
        histogram_weakening = hist_curr < hist_prev
        return macd_reversal or (histogram_weakening and adx_declining)

    macd_reversal = macd_curr > signal_curr
    histogram_weakening = hist_curr > hist_prev
    return macd_reversal or (histogram_weakening and adx_declining)


class RiskManager:
    """Optional wrapper class exposing all risk management methods."""

    calculate_sl_swing = staticmethod(calculate_sl_swing)
    calculate_sl_kijun = staticmethod(calculate_sl_kijun)
    calculate_sl_cloud = staticmethod(calculate_sl_cloud)
    calculate_sl_atr = staticmethod(calculate_sl_atr)
    calculate_sl_signal_candle = staticmethod(calculate_sl_signal_candle)
    calculate_sl_support_resistance = staticmethod(calculate_sl_support_resistance)
    check_exit_condition = staticmethod(check_exit_condition)

    calculate_tp_rr = staticmethod(calculate_tp_rr)
    calculate_tp_support_resistance = staticmethod(calculate_tp_support_resistance)
    calculate_tp_atr = staticmethod(calculate_tp_atr)
    calculate_tp_ichimoku = staticmethod(calculate_tp_ichimoku)
    calculate_partial_tp = staticmethod(calculate_partial_tp)
    update_trailing_sl_kijun = staticmethod(update_trailing_sl_kijun)
    check_tp_exit = staticmethod(check_tp_exit)


if __name__ == "__main__":
    # Example OHLCV + indicators data
    sample_df = pd.DataFrame(
        {
            "open": [100, 101, 102, 101, 103, 104, 105, 104, 106, 107, 108, 109],
            "high": [101, 102, 103, 103, 104, 105, 106, 106, 107, 108, 109, 110],
            "low": [99, 100, 101, 100, 102, 103, 104, 103, 105, 106, 107, 108],
            "close": [100.5, 101.5, 102.5, 102.0, 103.5, 104.5, 105.0, 105.5, 106.5, 107.5, 108.5, 109.5],
            "tick_volume": [1000, 1100, 1200, 1150, 1300, 1400, 1500, 1450, 1600, 1700, 1800, 1900],
            "adx": [20, 22, 24, 26, 28, 27, 26, 25, 24, 23, 22, 21],
            "plus_di": [18, 19, 21, 23, 25, 24, 23, 22, 21, 20, 19, 18],
            "minus_di": [22, 21, 19, 17, 15, 16, 17, 18, 19, 20, 21, 22],
            "macd": [0.1, 0.2, 0.25, 0.3, 0.35, 0.33, 0.31, 0.29, 0.26, 0.22, 0.18, 0.14],
            "signal": [0.08, 0.15, 0.2, 0.24, 0.28, 0.3, 0.3, 0.3, 0.28, 0.25, 0.22, 0.2],
            "histogram": [0.02, 0.05, 0.05, 0.06, 0.07, 0.03, 0.01, -0.01, -0.02, -0.03, -0.04, -0.06],
            "atr": [1.2, 1.25, 1.3, 1.35, 1.4, 1.38, 1.36, 1.34, 1.32, 1.3, 1.28, 1.26],
            "kijun": [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 103.5, 104.0, 104.5, 105.0, 105.5],
            "span_a": [100.2, 100.7, 101.2, 101.7, 102.2, 102.7, 103.2, 103.7, 104.2, 104.7, 105.2, 105.7],
            "span_b": [99.8, 100.3, 100.8, 101.3, 101.8, 102.3, 102.8, 103.3, 103.8, 104.3, 104.8, 105.3],
        }
    )

    entry_idx = 8
    current_idx = 11
    direction_val = "long"
    entry_price_val = float(sample_df.iloc[entry_idx]["close"])
    buffer_val = 0.1

    # Stop Loss examples
    sl_swing = calculate_sl_swing(sample_df, entry_idx, direction_val, buffer_val, lookback=10)
    sl_kijun = calculate_sl_kijun(sample_df, entry_idx, direction_val, buffer_val)
    sl_cloud = calculate_sl_cloud(sample_df, entry_idx, direction_val, buffer_val)
    sl_atr = calculate_sl_atr(sample_df, entry_idx, entry_price_val, direction_val, atr_multiplier=1.5)
    sl_signal = calculate_sl_signal_candle(sample_df, entry_idx, direction_val, buffer_val)
    sl_sr = calculate_sl_support_resistance(sample_df, entry_idx, direction_val, lookback=10, buffer=buffer_val)
    exit_cond = check_exit_condition(
        sample_df,
        entry_idx,
        current_idx,
        direction_val,
        max_bars=5,
        adx_threshold=23,
    )

    # Take Profit examples
    tp_rr = calculate_tp_rr(entry_price_val, sl_swing, direction_val, rr_ratio=2.0)
    tp_sr = calculate_tp_support_resistance(sample_df, entry_idx, direction_val, lookback=10)
    tp_atr = calculate_tp_atr(sample_df, entry_idx, entry_price_val, direction_val, multiplier=2.0)
    tp_ichi = calculate_tp_ichimoku(sample_df, entry_idx, direction_val)
    tp1, tp2 = calculate_partial_tp(entry_price_val, sl_swing, direction_val)
    trailed_sl = update_trailing_sl_kijun(sample_df, current_idx, direction_val, current_sl=sl_kijun)
    tp_exit = check_tp_exit(sample_df, current_idx, direction_val)

    print("SL Swing:", sl_swing)
    print("SL Kijun:", sl_kijun)
    print("SL Cloud:", sl_cloud)
    print("SL ATR:", sl_atr)
    print("SL Signal Candle:", sl_signal)
    print("SL Support/Resistance:", sl_sr)
    print("Conditional Exit:", exit_cond)

    print("TP RR:", tp_rr)
    print("TP Support/Resistance:", tp_sr)
    print("TP ATR:", tp_atr)
    print("TP Ichimoku:", tp_ichi)
    print("Partial TP1:", tp1)
    print("Partial TP2:", tp2)
    print("Trailing SL Kijun:", trailed_sl)
    print("Dynamic TP Exit:", tp_exit)