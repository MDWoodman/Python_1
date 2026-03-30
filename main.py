from __future__ import annotations

import json
from pathlib import Path

from api_broker.api_MT5 import API
from config import conf as cnf
from config.product_conf import ProductConf
import tools


DEFAULT_MCAD_SHORT = 12
DEFAULT_MCAD_LONG = 26
DEFAULT_MCAD_SIGNAL = 9
DEFAULT_MCAD_ANGLE = 25
DEFAULT_ADX_WINDOW = 14
DEFAULT_ADX_THRESHOLD = 30
DEFAULT_TENKAN = 9
DEFAULT_KIJUN = 26
DEFAULT_SENKOU_B = 52


def _build_default_products(symbols: list[str]) -> list[dict]:
    products: list[dict] = []
    for symbol in symbols:
        product = ProductConf(
            symbol=symbol,
            short_window_mcad=DEFAULT_MCAD_SHORT,
            long_window_mcad=DEFAULT_MCAD_LONG,
            signal_window_mcad=DEFAULT_MCAD_SIGNAL,
            angle_mcad=DEFAULT_MCAD_ANGLE,
            adx_window=DEFAULT_ADX_WINDOW,
            adx_adx=DEFAULT_ADX_THRESHOLD,
            tenkansen_period=DEFAULT_TENKAN,
            kijunsen_period=DEFAULT_KIJUN,
            senkouspan_period=DEFAULT_SENKOU_B,
        )
        products.append(product.to_dict())
    return products


def main() -> None:
    # Configure logging handlers used across the project.
    tools.logger_configuration()

    api_client = API(
        login=cnf.USERNAME,
        password=cnf.PASSWORD,
        server=cnf.MT5_SERVER,
        path=cnf.MT5_PATH,
    )

    try:
        symbols = list(cnf.SYMBOLS_LIST)
        products = _build_default_products(symbols)

        products_path = Path(__file__).resolve().parent / "config" / "products.json"
        products_path.write_text(json.dumps(products, indent=4), encoding="utf-8")
        print(f"Zapisano konfiguracje produktow: {products_path}")
    finally:
        api_client.shutdown()


if __name__ == "__main__":
    main()

