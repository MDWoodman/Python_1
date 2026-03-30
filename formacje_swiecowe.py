from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from candle import Candle


@dataclass
class CandlePatternResult:
    signal: str | None
    patterns: list[str]
    candle_time: int | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal": self.signal,
            "patterns": self.patterns,
            "candle_time": self.candle_time,
        }


def _body(candle: Candle) -> float:
    return abs(candle.close - candle.open)


def _range(candle: Candle) -> float:
    return max(candle.high - candle.low, 1e-10)


def _upper_shadow(candle: Candle) -> float:
    return candle.high - max(candle.open, candle.close)


def _lower_shadow(candle: Candle) -> float:
    return min(candle.open, candle.close) - candle.low


def _is_bullish(candle: Candle) -> bool:
    return candle.close > candle.open


def _is_bearish(candle: Candle) -> bool:
    return candle.close < candle.open


def is_hammer(candle: Candle) -> bool:
    body = _body(candle)
    rng = _range(candle)
    lower = _lower_shadow(candle)
    upper = _upper_shadow(candle)
    return body / rng <= 0.35 and lower >= body * 2 and upper <= body


def is_shooting_star(candle: Candle) -> bool:
    body = _body(candle)
    rng = _range(candle)
    lower = _lower_shadow(candle)
    upper = _upper_shadow(candle)
    return body / rng <= 0.35 and upper >= body * 2 and lower <= body


def is_bullish_engulfing(prev_candle: Candle, current_candle: Candle) -> bool:
    if not _is_bearish(prev_candle) or not _is_bullish(current_candle):
        return False
    return current_candle.open <= prev_candle.close and current_candle.close >= prev_candle.open


def is_bearish_engulfing(prev_candle: Candle, current_candle: Candle) -> bool:
    if not _is_bullish(prev_candle) or not _is_bearish(current_candle):
        return False
    return current_candle.open >= prev_candle.close and current_candle.close <= prev_candle.open


def is_morning_star(c1: Candle, c2: Candle, c3: Candle) -> bool:
    return _is_bearish(c1) and _body(c2) <= _body(c1) * 0.5 and _is_bullish(c3) and c3.close > (c1.open + c1.close) / 2


def is_evening_star(c1: Candle, c2: Candle, c3: Candle) -> bool:
    return _is_bullish(c1) and _body(c2) <= _body(c1) * 0.5 and _is_bearish(c3) and c3.close < (c1.open + c1.close) / 2


def _map_candle(raw_candle: Any) -> Candle:
    if isinstance(raw_candle, Candle):
        return raw_candle

    if isinstance(raw_candle, dict):
        return Candle(
            time=raw_candle.get("time"),
            open=float(raw_candle.get("open", 0.0)),
            high=float(raw_candle.get("high", raw_candle.get("height", 0.0))),
            low=float(raw_candle.get("low", 0.0)),
            close=float(raw_candle.get("close", 0.0)),
            tick_volume=float(raw_candle.get("tick_volume", raw_candle.get("volume", 0.0))),
        )

    if isinstance(raw_candle, (list, tuple)) and len(raw_candle) >= 6:
        return Candle(
            time=raw_candle[0],
            open=float(raw_candle[1]),
            close=float(raw_candle[2]),
            high=float(raw_candle[3]),
            low=float(raw_candle[4]),
            tick_volume=float(raw_candle[5]),
        )

    raise ValueError(f"Unsupported candle format: {type(raw_candle)}")


def analyze_open_signal(candles: list[Any]) -> dict[str, Any]:
    if len(candles) < 2:
        return CandlePatternResult(signal=None, patterns=[], candle_time=None).to_dict()

    mapped_candles = [_map_candle(candle) for candle in candles]

    patterns_buy: list[str] = []
    patterns_sell: list[str] = []

    last = mapped_candles[-1]
    prev = mapped_candles[-2]

    if is_hammer(last):
        patterns_buy.append("Hammer")
    if is_shooting_star(last):
        patterns_sell.append("ShootingStar")

    if is_bullish_engulfing(prev, last):
        patterns_buy.append("BullishEngulfing")
    if is_bearish_engulfing(prev, last):
        patterns_sell.append("BearishEngulfing")

    if len(mapped_candles) >= 3:
        c1, c2, c3 = mapped_candles[-3], mapped_candles[-2], mapped_candles[-1]
        if is_morning_star(c1, c2, c3):
            patterns_buy.append("MorningStar")
        if is_evening_star(c1, c2, c3):
            patterns_sell.append("EveningStar")

    if len(patterns_buy) > len(patterns_sell):
        return CandlePatternResult(signal="BUY", patterns=patterns_buy, candle_time=last.time).to_dict()

    if len(patterns_sell) > len(patterns_buy):
        return CandlePatternResult(signal="SELL", patterns=patterns_sell, candle_time=last.time).to_dict()

    return CandlePatternResult(signal=None, patterns=[], candle_time=last.time).to_dict()
