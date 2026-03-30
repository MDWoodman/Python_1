from collections.abc import Sequence
from enum import Enum
from typing import Any

import pandas as pd


class Trend(Enum):
    INCREASING = 1
    DECREASING = 2
    NEITHER = 3


class mcad_result_enum(Enum):
    Wzrost_przeciecie = 1
    Spadek_przeciecie = 2
    Boczny = 3


class mcad_analyze_result_object:
    def __init__(self, time: int, symbol: str, period: str, result: mcad_result_enum, trend: Trend):
        self.result = result
        self.time = time
        self.symbol = symbol
        self.period = period
        self.trend = trend
        self.raw_cross = None
        self.raw_cross_time = None

    def get_result(self):
        return self.result

    def get_time(self):
        return self.time

    def get_symbol(self):
        return self.symbol

    def get_period(self):
        return self.period

    def get_trend(self):
        return self.trend

    def set_raw_cross(self, cross: str | None, cross_time: int | None):
        self.raw_cross = cross
        self.raw_cross_time = cross_time

    def get_raw_cross(self):
        return self.raw_cross

    def get_raw_cross_time(self):
        return self.raw_cross_time


class mcad_object:
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
            return pd.DataFrame(
                {
                    "Date": [row.time for row in rows],
                    "Open": [row.open for row in rows],
                    "High": [row.high for row in rows],
                    "Low": [row.low for row in rows],
                    "Close": [row.close for row in rows],
                    "Volume": [row.tick_volume for row in rows],
                }
            )

        # SQLite row schema in this project: time, open, high, low, close, volume, timestr
        if len(first) >= 7:
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

    def calculate_mcad(
        self,
        records: pd.DataFrame | Sequence[Any],
        short_window: int = 12,
        long_window: int = 26,
        signal_window: int = 9,
    ) -> pd.DataFrame:
        df = self.get_data_from_candle_array(records)

        df["ShortEMA"] = df["Close"].ewm(span=int(short_window), adjust=False).mean()
        df["LongEMA"] = df["Close"].ewm(span=int(long_window), adjust=False).mean()
        df["MACD"] = df["ShortEMA"] - df["LongEMA"]
        df["Signal Line"] = df["MACD"].ewm(span=int(signal_window), adjust=False).mean()
        df["MACD_histogram"] = df["MACD"] - df["Signal Line"]

        return df

    def _latest_crossover_index(self, mcad_df: pd.DataFrame) -> int | None:
        if len(mcad_df) < 2:
            return None

        for idx in range(len(mcad_df) - 1, 0, -1):
            prev_row = mcad_df.iloc[idx - 1]
            row = mcad_df.iloc[idx]

            prev_macd = prev_row.get("MACD")
            prev_signal = prev_row.get("Signal Line")
            curr_macd = row.get("MACD")
            curr_signal = row.get("Signal Line")

            if any(pd.isna(value) for value in [prev_macd, prev_signal, curr_macd, curr_signal]):
                continue

            crossed_up = prev_macd <= prev_signal and curr_macd > curr_signal
            crossed_down = prev_macd >= prev_signal and curr_macd < curr_signal
            if crossed_up or crossed_down:
                return idx

        return None

    def _trend_from_mcad(self, mcad_df: pd.DataFrame) -> Trend:
        valid = mcad_df["MACD"].dropna()
        if len(valid) < 2:
            return Trend.NEITHER

        prev_value = float(valid.iloc[-2])
        last_value = float(valid.iloc[-1])

        if last_value > prev_value:
            return Trend.INCREASING
        if last_value < prev_value:
            return Trend.DECREASING
        return Trend.NEITHER

    def analyze_mcad(
        self,
        records: pd.DataFrame | Sequence[Any],
        short_window: int = 12,
        long_window: int = 26,
        signal_window: int = 9,
        angle: float = 45,
    ) -> tuple[mcad_result_enum, int | None, Trend, str | None, int | None]:
        _ = angle  # kept for compatibility with previous function signature
        mcad_df = self.calculate_mcad(records, short_window, long_window, signal_window)

        if mcad_df.empty:
            return mcad_result_enum.Boczny, None, Trend.NEITHER, None, None

        trend = self._trend_from_mcad(mcad_df)
        last_time = _to_epoch_ms(mcad_df.iloc[-1]["Date"])

        crossover_idx = self._latest_crossover_index(mcad_df)
        if crossover_idx is None:
            return mcad_result_enum.Boczny, last_time, trend, None, None

        prev_row = mcad_df.iloc[crossover_idx - 1]
        row = mcad_df.iloc[crossover_idx]
        cross_time = _to_epoch_ms(row["Date"])

        prev_macd = float(prev_row["MACD"])
        prev_signal = float(prev_row["Signal Line"])
        curr_macd = float(row["MACD"])
        curr_signal = float(row["Signal Line"])

        if prev_macd <= prev_signal and curr_macd > curr_signal:
            return mcad_result_enum.Wzrost_przeciecie, cross_time, trend, "WZROST", cross_time
        if prev_macd >= prev_signal and curr_macd < curr_signal:
            return mcad_result_enum.Spadek_przeciecie, cross_time, trend, "SPADEK", cross_time

        return mcad_result_enum.Boczny, last_time, trend, None, None


def _to_epoch_ms(value: Any) -> int:
    if isinstance(value, (int, float)):
        numeric = int(value)
        return numeric if numeric > 10_000_000_000 else numeric * 1000

    text = str(value).strip()
    if text.isdigit():
        numeric = int(text)
        return numeric if numeric > 10_000_000_000 else numeric * 1000

    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Nie mozna sparsowac czasu MCAD: {value}")
    return int(parsed.timestamp() * 1000)


def analyze_mcad_candles(
    data: pd.DataFrame | Sequence[Any],
    short_window: int,
    long_window: int,
    signal_window: int,
    angle: float,
    symbol: str,
    period: str,
) -> tuple[mcad_analyze_result_object | None, Trend | None]:
    safe_long = int(long_window or 26)
    safe_signal = int(signal_window or 9)
    min_len = max(3, safe_long + safe_signal)

    if data is None or len(data) < min_len:
        return None, None

    mcad_obj = mcad_object()
    mcad_result, time_result, trend, raw_cross, raw_cross_time = mcad_obj.analyze_mcad(
        data,
        short_window=short_window,
        long_window=long_window,
        signal_window=signal_window,
        angle=angle,
    )

    if time_result is None:
        return None, None

    analyze_result_obj = mcad_analyze_result_object(
        time=time_result,
        symbol=symbol,
        period=period,
        result=mcad_result,
        trend=trend,
    )
    analyze_result_obj.set_raw_cross(raw_cross, raw_cross_time)

    return analyze_result_obj, trend
