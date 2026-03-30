from enum import Enum
from collections.abc import Sequence
from typing import Any

import pandas as pd
import numpy as np

class adx_object :
    def _wilder_smoothing_sum(self, series: pd.Series, period: int) -> pd.Series:
        values = series.to_numpy(dtype=float)
        result = np.full(len(values), np.nan, dtype=float)

        if period <= 0 or len(values) < period:
            return pd.Series(result, index=series.index)

        initial_sum = np.nansum(values[:period])
        result[period - 1] = initial_sum

        for idx in range(period, len(values)):
            result[idx] = result[idx - 1] - (result[idx - 1] / period) + values[idx]

        return pd.Series(result, index=series.index)


    def _wilder_smoothing_avg(self, series: pd.Series, period: int) -> pd.Series:
        values = series.to_numpy(dtype=float)
        result = np.full(len(values), np.nan, dtype=float)

        if period <= 0 or len(values) < period:
            return pd.Series(result, index=series.index)

        initial_avg = np.nanmean(values[:period])
        result[period - 1] = initial_avg

        for idx in range(period, len(values)):
            result[idx] = ((result[idx - 1] * (period - 1)) + values[idx]) / period

        return pd.Series(result, index=series.index)

    def calculate_adx(self, df: pd.DataFrame | Sequence[Any], period: int = 14) -> pd.DataFrame:
        """
        Oblicza ADX, +DI, -DI.
        Funkcja przyjmuje DataFrame lub sekwencję świec i normalizuje wejście
        do kolumn: Date, Open, High, Low, Close, Volume.
        """
        safe_period = int(period or 14)
        if safe_period <= 0:
            raise ValueError("period must be > 0")

        df = self.get_data_from_candle_array(df)

        previous_close = df["Close"].shift(1)

        # True Range
        tr_components = pd.concat(
            [
                (df["High"] - df["Low"]).abs(),
                (df["High"] - previous_close).abs(),
                (df["Low"] - previous_close).abs(),
            ],
            axis=1,
        )
        df["TR"] = tr_components.max(axis=1)

        # Directional Movement
        up_move = df["High"].diff()
        down_move = -df["Low"].diff()

        df["+DM"] = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        df["-DM"] = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

        # Wilder smoothing (sum for TR/DM, avg for ADX)
        df["TR_smooth"] = self._wilder_smoothing_sum(df["TR"], safe_period)
        df["+DM_smooth"] = self._wilder_smoothing_sum(df["+DM"], safe_period)
        df["-DM_smooth"] = self._wilder_smoothing_sum(df["-DM"], safe_period)

        tr_non_zero = df["TR_smooth"].replace(0, np.nan)
        df["+DI"] = 100 * (df["+DM_smooth"] / tr_non_zero)
        df["-DI"] = 100 * (df["-DM_smooth"] / tr_non_zero)

        di_sum = (df["+DI"] + df["-DI"]).replace(0, np.nan)
        df["DX"] = 100 * (df["+DI"] - df["-DI"]).abs() / di_sum

        df["ADX"] = self._wilder_smoothing_avg(df["DX"], safe_period)

        return df


# ===== wczytanie danych z MT5

    # =========================
    # SIGNAL FUNCTIONS
    # =========================

    def buy_signal(self, df: pd.DataFrame, adx_threshold: float = 25) -> bool:
        """
        Sygnał kupna:
        1. +DI przecina -DI w górę
        2. ADX > próg
        3. ADX rośnie
        """

        if len(df) < 3:
            return False

        last = df.iloc[-1]
        prev = df.iloc[-2]

        if pd.isna(prev["+DI"]) or pd.isna(prev["-DI"]) or pd.isna(last["+DI"]) or pd.isna(last["-DI"]) or pd.isna(prev["ADX"]) or pd.isna(last["ADX"]):
            return False

        di_cross = prev["+DI"] <= prev["-DI"] and last["+DI"] > last["-DI"]
        strong_trend = last["ADX"] > adx_threshold
        adx_rising = last["ADX"] > prev["ADX"]

        return di_cross and strong_trend and adx_rising


    def sell_signal(self, df: pd.DataFrame, adx_threshold: float = 25) -> bool:
        """
        Sygnał sprzedaży:
        1. -DI przecina +DI w górę
        2. ADX > próg
        3. ADX rośnie
        """

        if len(df) < 3:
            return False

        last = df.iloc[-1]
        prev = df.iloc[-2]

        if pd.isna(prev["+DI"]) or pd.isna(prev["-DI"]) or pd.isna(last["+DI"]) or pd.isna(last["-DI"]) or pd.isna(prev["ADX"]) or pd.isna(last["ADX"]):
            return False

        di_cross = prev["-DI"] <= prev["+DI"] and last["-DI"] > last["+DI"]
        strong_trend = last["ADX"] > adx_threshold
        adx_rising = last["ADX"] > prev["ADX"]

        return di_cross and strong_trend and adx_rising


    # =========================
    # EXTENDED FILTER VERSION
    # =========================

    def trend_strength(self, df: pd.DataFrame) -> str:
        """
        Określa siłę trendu
        """

        last_adx = df.iloc[-1]["ADX"]

        if last_adx < 20:
            return "NO_TREND"
        elif 20 <= last_adx < 25:
            return "WEAK_TREND"
        elif 25 <= last_adx < 40:
            return "STRONG_TREND"
        else:
            return "VERY_STRONG_TREND"
        
    def get_data_from_candle_array(self, records: pd.DataFrame | Sequence[Any]) -> pd.DataFrame:
            if isinstance(records, pd.DataFrame):
                return records[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()

            rows = list(records)
            if not rows:
                return pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close", "Volume"])

            first = rows[0]

            if isinstance(first, dict):
                return pd.DataFrame(rows)[["Date", "Open", "High", "Low", "Close", "Volume"]]

            if all(hasattr(first, attr) for attr in ["time", "open", "high", "low", "close", "tick_volume"]):
                data = {
                    "Date": [row.time for row in rows],
                    "Open": [row.open for row in rows],
                    "High": [row.high for row in rows],
                    "Low": [row.low for row in rows],
                    "Close": [row.close for row in rows],
                    "Volume": [row.tick_volume for row in rows],
                }
                return pd.DataFrame(data)

            first_len = len(first)

            if first_len >= 7:
                return pd.DataFrame(
                    {
                        "Date": [row[0] for row in rows],
                        "Open": [row[1] for row in rows],
                        "High": [row[2] for row in rows],
                        "Low": [row[3] for row in rows],
                        "Close": [row[4] for row in rows],
                        "Volume": [row[5] for row in rows],
                    }
                )

            return pd.DataFrame(
                {
                    "Date": [row[0] for row in rows],
                    "Open": [row[1] for row in rows],
                    "High": [row[2] for row in rows],
                    "Low": [row[3] for row in rows],
                    "Close": [row[4] for row in rows],
                    "Volume": [row[5] for row in rows],
                }
            )

class adx_result_enum(Enum):
    Wzrost_przeciecie = 1
    Spadek_przeciecie = 2
    Boczny = 3

class Trend(Enum):
    INCREASING = 1
    DECREASING = 2
    NEITHER = 3

class adx_analyze_result_object:    
    def __init__(self, time, symbol, period, result: adx_result_enum , trend: Trend):
        self.result = result
        self.time = time
        self.symbol = symbol
        self.period = period
        self.trend = trend
        self.raw_di_cross = None
        self.raw_di_cross_time = None

    def get_time(self):
        return self.time

    def get_symbol(self):
        return self.symbol

    def get_period(self):
        return self.period

    def get_result(self):
        return self.result
    
    def get_trend(self):
        return self.trend

    def set_raw_di_cross(self, cross: str | None, cross_time: int | None):
        self.raw_di_cross = cross
        self.raw_di_cross_time = cross_time

    def get_raw_di_cross(self):
        return self.raw_di_cross

    def get_raw_di_cross_time(self):
        return self.raw_di_cross_time


def _to_epoch_ms(value: Any) -> int:
    if isinstance(value, (int, float, np.integer, np.floating)):
        numeric = int(value)
        return numeric if numeric > 10_000_000_000 else numeric * 1000

    text = str(value).strip()
    if text.isdigit():
        numeric = int(text)
        return numeric if numeric > 10_000_000_000 else numeric * 1000

    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Nie można sparsować czasu ADX: {value}")
    return int(parsed.timestamp() * 1000)


def analyze_adx_candles(
    data: pd.DataFrame | Sequence[Any],
    adx_window: int,
    adx_threshold: float,
    symbol: str,
    period: str,
) -> tuple[adx_analyze_result_object | None, Trend | None]:
    safe_window = int(adx_window or 14)
    if data is None or len(data) < max(3, safe_window):
        return None, None

    adx_obj = adx_object()
    adx_data = adx_obj.get_data_from_candle_array(data)
    adx_df = adx_obj.calculate_adx(adx_data, period=safe_window)

    if adx_df.empty or len(adx_df) < 3:
        return None, None

    valid_adx = adx_df["ADX"].dropna()
    if len(valid_adx) < 2:
        return None, None

    last_adx = float(valid_adx.iloc[-1])
    prev_adx = float(valid_adx.iloc[-2])

    if last_adx > prev_adx:
        adx_trend = Trend.INCREASING
    elif last_adx < prev_adx:
        adx_trend = Trend.DECREASING
    else:
        adx_trend = Trend.NEITHER

    _ = adx_threshold  # kept for compatibility; classification is based only on DI cross.
    adx_result = adx_result_enum.Boczny

    raw_di_cross = None
    raw_di_cross_time = None
    if len(adx_df) >= 2:
        prev_di = adx_df.iloc[-2]
        last_di = adx_df.iloc[-1]

        prev_plus_di = prev_di.get("+DI")
        prev_minus_di = prev_di.get("-DI")
        last_plus_di = last_di.get("+DI")
        last_minus_di = last_di.get("-DI")

        if (
            pd.notna(prev_plus_di)
            and pd.notna(prev_minus_di)
            and pd.notna(last_plus_di)
            and pd.notna(last_minus_di)
        ):
            if prev_plus_di <= prev_minus_di and last_plus_di > last_minus_di:
                raw_di_cross = "WZROST"
                raw_di_cross_time = _to_epoch_ms(adx_data.iloc[-1]["Date"])
            elif prev_plus_di >= prev_minus_di and last_plus_di < last_minus_di:
                raw_di_cross = "SPADEK"
                raw_di_cross_time = _to_epoch_ms(adx_data.iloc[-1]["Date"])

    if raw_di_cross == "WZROST":
        adx_result = adx_result_enum.Wzrost_przeciecie
    elif raw_di_cross == "SPADEK":
        adx_result = adx_result_enum.Spadek_przeciecie

    time_result = _to_epoch_ms(adx_data.iloc[-1]["Date"])

    adx_analyze_result_obj = adx_analyze_result_object(
        time_result,
        symbol,
        period,
        adx_result,
        adx_trend,
    )
    adx_analyze_result_obj.set_raw_di_cross(raw_di_cross, raw_di_cross_time)

    return adx_analyze_result_obj, adx_trend