from __future__ import annotations

from collections.abc import Sequence
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd


class Trend(Enum):
    INCREASING = 1
    DECREASING = 2
    NEITHER = 3


class ichi_crossover_price_kiusen_result_enum(Enum):
    Przeciecie_do_gory = 1
    Przeciecie_do_dolu = 2
    Brak_przeciecia = 3


class ichi_crossover_price_senokuspan_result_enum(Enum):
    Przeciecie_do_gory = 1
    Przeciecie_do_dolu = 2
    Brak_przeciecia = 3


class ichi_crossover_tenkansen_kiusen_result_enum(Enum):
    Przeciecie_do_gory = 1
    Przeciecie_do_dolu = 2
    Brak_przeciecia = 3


class ichi_result_object:
    def __init__(self):
        self._crossover_result_tenkansen_kiusen: ichi_crossover_tenkansen_kiusen_result_enum | None = None
        self._time_of_cross_tenkansen_kiusen: int | None = None

        self._crossover_result_price_kiusen: ichi_crossover_price_kiusen_result_enum | None = None
        self._time_of_cross_price_kiusen: int | None = None

        self._crossover_price_senokuspan: ichi_crossover_price_senokuspan_result_enum | None = None
        self._time_of_cross_price_senokuspan: int | None = None

    @property
    def crossover_result_tenkansen_kiusen(self):
        return self._crossover_result_tenkansen_kiusen

    @crossover_result_tenkansen_kiusen.setter
    def crossover_result_tenkansen_kiusen(self, value):
        self._crossover_result_tenkansen_kiusen = value

    @property
    def time_of_cross_tenkansen_kiusen(self):
        return self._time_of_cross_tenkansen_kiusen

    @time_of_cross_tenkansen_kiusen.setter
    def time_of_cross_tenkansen_kiusen(self, value):
        self._time_of_cross_tenkansen_kiusen = value

    @property
    def crossover_result_price_kiusen(self):
        return self._crossover_result_price_kiusen

    @crossover_result_price_kiusen.setter
    def crossover_result_price_kiusen(self, value):
        self._crossover_result_price_kiusen = value

    @property
    def time_of_cross_price_kiusen(self):
        return self._time_of_cross_price_kiusen

    @time_of_cross_price_kiusen.setter
    def time_of_cross_price_kiusen(self, value):
        self._time_of_cross_price_kiusen = value

    @property
    def crossover_price_senokuspan(self):
        return self._crossover_price_senokuspan

    @crossover_price_senokuspan.setter
    def crossover_price_senokuspan(self, value):
        self._crossover_price_senokuspan = value

    @property
    def time_of_cross_price_senokuspan(self):
        return self._time_of_cross_price_senokuspan

    @time_of_cross_price_senokuspan.setter
    def time_of_cross_price_senokuspan(self, value):
        self._time_of_cross_price_senokuspan = value


class ichimoku_analyze_result_object:
    def __init__(self, time: int, symbol: str, period: str, result: ichi_result_object):
        self._symbol = symbol
        self._period = period
        self._result = result
        self._time = time

    @property
    def symbol(self):
        return self._symbol

    @property
    def period(self):
        return self._period

    @property
    def result(self):
        return self._result

    def get_time(self):
        return self._time

    def get_result(self):
        return self._result

    def get_symbol(self):
        return self._symbol

    def get_period(self):
        return self._period


class ichimoku_object:
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

    def calculate_ichimoku(
        self,
        records: pd.DataFrame | Sequence[Any],
        tenkansen: int = 9,
        kiusen: int = 26,
        senokuspan: int = 52,
    ) -> pd.DataFrame:
        df = self.get_data_from_candle_array(records)

        safe_tenkan = max(1, int(tenkansen or 9))
        safe_kijun = max(1, int(kiusen or 26))
        safe_span_b = max(1, int(senokuspan or 52))

        high = df["High"].astype(float)
        low = df["Low"].astype(float)

        df["Tenkan_sen"] = (high.rolling(window=safe_tenkan).max() + low.rolling(window=safe_tenkan).min()) / 2
        df["Kijun_sen"] = (high.rolling(window=safe_kijun).max() + low.rolling(window=safe_kijun).min()) / 2
        df["Senkou_Span_A"] = ((df["Tenkan_sen"] + df["Kijun_sen"]) / 2).shift(safe_kijun)
        df["Senkou_Span_B"] = ((high.rolling(window=safe_span_b).max() + low.rolling(window=safe_span_b).min()) / 2).shift(
            safe_kijun
        )
        df["Chikou_Span"] = df["Close"].astype(float).shift(-safe_kijun)

        return df

    def _latest_line_crossover(
        self,
        fast_line: pd.Series,
        slow_line: pd.Series,
        dates: pd.Series,
    ) -> tuple[ichi_crossover_tenkansen_kiusen_result_enum, int | None]:
        for idx in range(len(fast_line) - 1, 0, -1):
            prev_fast = fast_line.iloc[idx - 1]
            prev_slow = slow_line.iloc[idx - 1]
            curr_fast = fast_line.iloc[idx]
            curr_slow = slow_line.iloc[idx]

            if any(pd.isna(v) for v in [prev_fast, prev_slow, curr_fast, curr_slow]):
                continue

            crossed_up = prev_fast <= prev_slow and curr_fast > curr_slow
            crossed_down = prev_fast >= prev_slow and curr_fast < curr_slow
            if crossed_up:
                return ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_gory, _to_epoch_ms(dates.iloc[idx])
            if crossed_down:
                return ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_dolu, _to_epoch_ms(dates.iloc[idx])

        return ichi_crossover_tenkansen_kiusen_result_enum.Brak_przeciecia, None

    def _latest_price_vs_line_cross(
        self,
        candle_df: pd.DataFrame,
        line_df: pd.DataFrame,
        line_column: str,
    ) -> tuple[ichi_crossover_price_kiusen_result_enum, int | None]:
        if candle_df.empty:
            return ichi_crossover_price_kiusen_result_enum.Brak_przeciecia, None

        merged = candle_df.merge(line_df[["Date", line_column]], on="Date", how="left")

        for idx in range(len(merged) - 1, -1, -1):
            row = merged.iloc[idx]
            line_value = row[line_column]
            if pd.isna(line_value):
                continue

            open_price = float(row["Open"])
            close_price = float(row["Close"])
            candle_time = _to_epoch_ms(row["Date"])

            if open_price <= line_value < close_price:
                return ichi_crossover_price_kiusen_result_enum.Przeciecie_do_gory, candle_time
            if open_price >= line_value > close_price:
                return ichi_crossover_price_kiusen_result_enum.Przeciecie_do_dolu, candle_time

        return ichi_crossover_price_kiusen_result_enum.Brak_przeciecia, _to_epoch_ms(merged.iloc[-1]["Date"])

    def _latest_price_vs_cloud_cross(
        self,
        candle_df: pd.DataFrame,
        line_df: pd.DataFrame,
    ) -> tuple[ichi_crossover_price_senokuspan_result_enum, int | None]:
        if candle_df.empty:
            return ichi_crossover_price_senokuspan_result_enum.Brak_przeciecia, None

        merged = candle_df.merge(line_df[["Date", "Senkou_Span_A", "Senkou_Span_B"]], on="Date", how="left")

        for idx in range(len(merged) - 1, -1, -1):
            row = merged.iloc[idx]
            open_price = float(row["Open"])
            close_price = float(row["Close"])
            candle_time = _to_epoch_ms(row["Date"])

            span_a = row["Senkou_Span_A"]
            span_b = row["Senkou_Span_B"]

            crosses_up = False
            crosses_down = False

            if pd.notna(span_a):
                crosses_up = crosses_up or (open_price <= span_a < close_price)
                crosses_down = crosses_down or (open_price >= span_a > close_price)

            if pd.notna(span_b):
                crosses_up = crosses_up or (open_price <= span_b < close_price)
                crosses_down = crosses_down or (open_price >= span_b > close_price)

            if crosses_up:
                return ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_gory, candle_time
            if crosses_down:
                return ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_dolu, candle_time

        return ichi_crossover_price_senokuspan_result_enum.Brak_przeciecia, _to_epoch_ms(merged.iloc[-1]["Date"])

    def analyze_ichimoku(
        self,
        stock_data_in: pd.DataFrame | Sequence[Any],
        last_n_candles: pd.DataFrame | Sequence[Any],
        tenkansen_period: int,
        kiusen_period: int,
        senokuspanB_period: int,
    ) -> ichi_result_object:
        ichi_df = self.calculate_ichimoku(
            stock_data_in,
            tenkansen=tenkansen_period,
            kiusen=kiusen_period,
            senokuspan=senokuspanB_period,
        )
        last_df = self.get_data_from_candle_array(last_n_candles)

        result = ichi_result_object()

        tenkan_kijun_result, tenkan_kijun_time = self._latest_line_crossover(
            ichi_df["Tenkan_sen"],
            ichi_df["Kijun_sen"],
            ichi_df["Date"],
        )
        result.crossover_result_tenkansen_kiusen = tenkan_kijun_result
        result.time_of_cross_tenkansen_kiusen = tenkan_kijun_time

        price_kijun_result, price_kijun_time = self._latest_price_vs_line_cross(
            last_df,
            ichi_df,
            "Kijun_sen",
        )
        result.crossover_result_price_kiusen = price_kijun_result
        result.time_of_cross_price_kiusen = price_kijun_time

        cloud_result, cloud_time = self._latest_price_vs_cloud_cross(last_df, ichi_df)
        result.crossover_price_senokuspan = cloud_result
        result.time_of_cross_price_senokuspan = cloud_time

        return result


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
        raise ValueError(f"Nie mozna sparsowac czasu Ichimoku: {value}")
    return int(parsed.timestamp() * 1000)


def analyze_ichimoku_candles(
    data: pd.DataFrame | Sequence[Any],
    last_n_candles: pd.DataFrame | Sequence[Any],
    tenkansen_period: int,
    kiusen_period: int,
    senokuspanB_period: int,
    symbol: str,
    period: str,
) -> tuple[ichimoku_analyze_result_object | None, Trend | None]:
    if data is None or len(data) < max(3, int(senokuspanB_period or 52)):
        return None, None

    ichi_obj = ichimoku_object()
    result = ichi_obj.analyze_ichimoku(
        data,
        last_n_candles,
        tenkansen_period=tenkansen_period,
        kiusen_period=kiusen_period,
        senokuspanB_period=senokuspanB_period,
    )

    data_df = ichi_obj.get_data_from_candle_array(data)
    if data_df.empty:
        return None, None

    calc_df = ichi_obj.calculate_ichimoku(
        data_df,
        tenkansen=tenkansen_period,
        kiusen=kiusen_period,
        senokuspan=senokuspanB_period,
    )

    valid_kijun = calc_df["Kijun_sen"].dropna()
    if len(valid_kijun) >= 2:
        if float(valid_kijun.iloc[-1]) > float(valid_kijun.iloc[-2]):
            trend = Trend.INCREASING
        elif float(valid_kijun.iloc[-1]) < float(valid_kijun.iloc[-2]):
            trend = Trend.DECREASING
        else:
            trend = Trend.NEITHER
    else:
        trend = Trend.NEITHER

    analyze_result = ichimoku_analyze_result_object(
        time=_to_epoch_ms(data_df.iloc[-1]["Date"]),
        symbol=symbol,
        period=period,
        result=result,
    )

    return analyze_result, trend
