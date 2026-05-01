from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Any

try:
    import MetaTrader5 as mt5
except Exception:
    mt5 = None


class API:
    def __init__(
        self,
        login: int | None = None,
        password: str | None = None,
        server: str | None = None,
        path: str | None = None,
        portable: bool = False,
        timeout: int = 60_000,
    ) -> None:
        self.connected = False
        self._initialize(login, password, server, path, portable, timeout)

    def _initialize(
        self,
        login: int | None,
        password: str | None,
        server: str | None,
        path: str | None,
        portable: bool,
        timeout: int,
    ) -> None:
        if mt5 is None:
            raise ImportError(
                "Brak pakietu 'MetaTrader5'. Zainstaluj: pip install MetaTrader5"
            )


        init_kwargs = {
            "timeout": int(timeout),
            "portable": bool(portable),
        }
        if path:
            init_kwargs["path"] = path

        init_ok = mt5.initialize(**init_kwargs)
        if not init_ok:
            code, msg = mt5.last_error()
            raise RuntimeError(
                f"MT5 initialize failed: [{code}] {msg}. "
                "Sprawdź czy terminal MT5 jest zainstalowany, 64-bit i ścieżka MT5_PATH jest poprawna."
            )

        login_value = self._normalize_login(login)
        if login_value is not None:
            login_kwargs = {
                "login": login_value,
                "password": password or "",
            }
            if server:
                login_kwargs["server"] = server

            login_ok = mt5.login(**login_kwargs)
            if not login_ok:
                code, msg = mt5.last_error()
                raise RuntimeError(
                    f"MT5 login failed: [{code}] {msg}. "
                    "Sprawdź login, hasło, server (np. broker-demo) i czy konto jest aktywne."
                )

        self.connected = True

    @staticmethod
    def _normalize_login(login: int | str | None) -> int | None:
        if login is None:
            return None
        if isinstance(login, int):
            return login

        login_text = str(login).strip()
        if login_text == "":
            return None
        if not login_text.isdigit():
            raise ValueError("MT5 login musi być numerem konta (tylko cyfry).")
        return int(login_text)

    def shutdown(self) -> None:
        if mt5 is not None:
            mt5.shutdown()
        self.connected = False

    def _ensure_connected(self) -> None:
        if not self.connected:
            raise RuntimeError("Brak połączenia z MT5. Użyj API(...) aby zainicjalizować terminal.")

    @staticmethod
    def _period_to_timeframe(period: Any) -> int:
        period_map = {
            "M1": mt5.TIMEFRAME_M1,
            "M2": mt5.TIMEFRAME_M2,
            "M3": mt5.TIMEFRAME_M3,
            "M4": mt5.TIMEFRAME_M4,
            "M5": mt5.TIMEFRAME_M5,
            "M6": mt5.TIMEFRAME_M6,
            "M10": mt5.TIMEFRAME_M10,
            "M12": mt5.TIMEFRAME_M12,
            "M15": mt5.TIMEFRAME_M15,
            "M20": mt5.TIMEFRAME_M20,
            "M30": mt5.TIMEFRAME_M30,
            "H1": mt5.TIMEFRAME_H1,
            "H2": mt5.TIMEFRAME_H2,
            "H3": mt5.TIMEFRAME_H3,
            "H4": mt5.TIMEFRAME_H4,
            "H6": mt5.TIMEFRAME_H6,
            "H8": mt5.TIMEFRAME_H8,
            "H12": mt5.TIMEFRAME_H12,
            "D1": mt5.TIMEFRAME_D1,
            "W1": mt5.TIMEFRAME_W1,
            "MN1": mt5.TIMEFRAME_MN1,
            1: mt5.TIMEFRAME_M1,
            5: mt5.TIMEFRAME_M5,
            15: mt5.TIMEFRAME_M15,
            30: mt5.TIMEFRAME_M30,
            60: mt5.TIMEFRAME_H1,
            240: mt5.TIMEFRAME_H4,
            1440: mt5.TIMEFRAME_D1,
        }
        if period not in period_map:
            raise ValueError(f"Unsupported period for MT5: {period}")
        return period_map[period]

    @staticmethod
    def _to_datetime(value: int | float | datetime) -> datetime:
        if isinstance(value, datetime):
            return value
        numeric = float(value)
        if numeric > 10_000_000_000:
            numeric = numeric / 1000.0
        # MT5 rate times are unix timestamps; use UTC to avoid local TZ offsets.
        return datetime.fromtimestamp(numeric, tz=timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _rate_to_dict(rate: Any) -> dict[str, Any]:
        return {
            # Keep epoch milliseconds to avoid timezone shifts during parse/format.
            "time": int(rate["time"]) * 1000,
            "open": float(rate["open"]),
            "high": float(rate["high"]),
            "low": float(rate["low"]),
            "close": float(rate["close"]),
            "tick_volume": float(rate["tick_volume"]),
        }

    def get_chart_range(self, symbol: str, period: str | int, start: int, end: int):
        self._ensure_connected()
        timeframe = self._period_to_timeframe(period)

        if not mt5.symbol_select(symbol, True):
            return None

        date_from = self._to_datetime(start)
        date_to = self._to_datetime(end)
        rates = mt5.copy_rates_range(symbol, timeframe, date_from, date_to)
        if rates is None or len(rates) == 0:
            return None

        return [self._rate_to_dict(rate) for rate in rates]

    def get_last_candle(self, symbol: str, period: str | int):
        self._ensure_connected()
        timeframe = self._period_to_timeframe(period)

        if not mt5.symbol_select(symbol, True):
            return None

        rates = mt5.copy_rates_from_pos(symbol, timeframe, 1, 1)
        if rates is None or len(rates) == 0:
            return None

        return self._rate_to_dict(rates[-1])

    def get_symbol_lot_info(self, symbol: str):
        self._ensure_connected()

        if not mt5.symbol_select(symbol, True):
            return {"min_lot": 0.01, "max_lot": 100.0, "lot_step": 0.01}

        info = mt5.symbol_info(symbol)
        if info is None:
            return {"min_lot": 0.01, "max_lot": 100.0, "lot_step": 0.01}

        return {
            "min_lot": float(info.volume_min),
            "max_lot": float(info.volume_max),
            "lot_step": float(info.volume_step),
        }

    def _resolve_order_type(self, action: str) -> int:
        action_normalized = action.lower().strip()
        if action_normalized == "buy":
            return mt5.ORDER_TYPE_BUY
        if action_normalized == "sell":
            return mt5.ORDER_TYPE_SELL
        raise ValueError("action must be 'buy' or 'sell'")

    @staticmethod
    def _step_decimals(step: float) -> int:
        step_text = f"{float(step):.10f}".rstrip("0")
        if "." not in step_text:
            return 0
        return len(step_text.split(".")[1])

    def _normalize_lot_size_for_symbol(self, symbol: str, requested_lot: float) -> float:
        lot_info = self.get_symbol_lot_info(symbol)

        min_lot = float(lot_info.get("min_lot", 0.01) or 0.01)
        max_lot = float(lot_info.get("max_lot", min_lot) or min_lot)
        lot_step = float(lot_info.get("lot_step", 0.01) or 0.01)

        if lot_step <= 0:
            lot_step = 0.01
        if max_lot < min_lot:
            max_lot = min_lot

        requested = float(requested_lot)
        if requested <= 0:
            requested = min_lot

        bounded = min(max(requested, min_lot), max_lot)

        steps_from_min = math.floor(((bounded - min_lot) / lot_step) + 1e-12)
        normalized = min_lot + (steps_from_min * lot_step)
        normalized = min(max(normalized, min_lot), max_lot)

        decimals = self._step_decimals(lot_step)
        return round(normalized, decimals)

    def open_transaction(
        self,
        action,
        _type,
        symbol,
        price,
        stop_loss,
        take_profit,
        comment,
        lot_size,
        magic,
        ticket,
    ):
        self._ensure_connected()

        if not mt5.symbol_select(symbol, True):
            return None

        normalized_lot = self._normalize_lot_size_for_symbol(symbol, lot_size)

        order_type = self._resolve_order_type(action)
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            return None

        requested_price = float(price) if price not in (None, 0) else (tick.ask if order_type == mt5.ORDER_TYPE_BUY else tick.bid)

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(normalized_lot),
            "type": order_type,
            "price": requested_price,
            "sl": float(stop_loss) if stop_loss else 0.0,
            "tp": float(take_profit) if take_profit else 0.0,
            "deviation": 20,
            "magic": int(magic) if magic is not None else 0,
            "comment": str(comment) if comment is not None else "",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        result = mt5.order_send(request)
        if result is None:
            return None
        return result._asdict()

    def close_transaction(self, ticket: int, volume: float = None, comment: str = ""):
        self._ensure_connected()

        positions = mt5.positions_get(ticket=ticket)
        if positions is None or len(positions) == 0:
            return None

        pos = positions[0]
        symbol = pos.symbol
        position_volume = float(pos.volume)
        close_volume = position_volume if volume is None else float(volume)

        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            return None

        close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.POSITION_TYPE_BUY else mt5.ORDER_TYPE_BUY
        close_price = tick.bid if close_type == mt5.ORDER_TYPE_SELL else tick.ask

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": close_volume,
            "type": close_type,
            "position": int(ticket),
            "price": close_price,
            "deviation": 20,
            "magic": int(pos.magic),
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        result = mt5.order_send(request)
        if result is None:
            return None
        return result._asdict()
