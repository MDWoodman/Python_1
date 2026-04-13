"""
Test otwarcia i zamkniecia transakcji w MT5.
Uzywa konta DEMO i minimalnego wolumenu (0.01 lota).
Uruchom recznie: python test_mt5_open_close.py
"""

from __future__ import annotations

from config import conf as cnf
from api_broker import api_MT5 as api_mt5

TEST_SYMBOL = "STOXX50"
TEST_LOT = 0.1
TEST_MAGIC = 999999
TEST_ACTION = "buy"  # zmien na "sell" zeby przetestowac sprzedaz


def _retcode_desc(retcode: int) -> str:
    descriptions = {
        10009: "SUKCES - zlecenie wykonane",
        10027: "BLAD - AutoTrading wylaczony w terminalu",
        10016: "BLAD - Nieprawidlowy SL/TP",
        10014: "BLAD - Nieprawidlowy wolumen",
        10019: "BLAD - Brak srodkow",
        10018: "BLAD - Rynek zamkniety",
        10030: "BLAD - Nieobslugiwany typ filling (ORDER_FILLING_IOC)",
        10015: "BLAD - Nieprawidlowa cena",
        10021: "BLAD - Brak kwotowan",
    }
    return descriptions.get(retcode, f"Nieznany retcode={retcode}")


def run_test():
    print("=" * 60)
    print("TEST MT5 - OTWARCIE I ZAMKNIECIE TRANSAKCJI")
    print(f"Symbol: {TEST_SYMBOL} | Akcja: {TEST_ACTION.upper()} | Lot: {TEST_LOT}")
    print("=" * 60)

    print("\n[1/4] Laczenie z MT5...")
    try:
        api = api_mt5.API(
            login=cnf.USERNAME,
            password=cnf.PASSWORD,
            server=cnf.MT5_SERVER,
            path=cnf.MT5_PATH,
        )
        print("     OK - polaczono z MT5")
    except Exception as e:
        print(f"     BLAD - nie mozna polaczyc z MT5: {e}")
        return

    print(f"\n[2/4] Pobieranie biezacej ceny {TEST_SYMBOL}...")
    try:
        import MetaTrader5 as mt5
        mt5.symbol_select(TEST_SYMBOL, True)
        tick = mt5.symbol_info_tick(TEST_SYMBOL)
        if tick is None:
            print(f"     BLAD - brak kwotowan dla {TEST_SYMBOL}")
            return
        print(f"     OK - ask={tick.ask:.2f}  bid={tick.bid:.2f}")

        if TEST_ACTION == "buy":
            entry_price = tick.ask
            sl = round(entry_price - 40, 2)
            tp = round(entry_price + 80, 2)
        else:
            entry_price = tick.bid
            sl = round(entry_price + 40, 2)
            tp = round(entry_price - 80, 2)
        print(f"     Planowane: entry={entry_price:.2f}  SL={sl:.2f}  TP={tp:.2f}")
    except Exception as e:
        print(f"     BLAD - {e}")
        return

    print(f"\n[3/4] Otwieranie transakcji {TEST_ACTION.upper()}...")
    try:
        mt5_result = api.open_transaction(
            action=TEST_ACTION,
            _type=None,
            symbol=TEST_SYMBOL,
            price=0,
            stop_loss=sl,
            take_profit=tp,
            comment="TEST_OPEN_CLOSE",
            lot_size=TEST_LOT,
            magic=TEST_MAGIC,
            ticket=None,
        )

        if mt5_result is None:
            print("     BLAD - open_transaction zwrocilo None (sprawdz polaczenie/symbol)")
            return

        retcode = mt5_result.get("retcode")
        ticket = mt5_result.get("order")
        print(f"     retcode={retcode} -> {_retcode_desc(retcode)}")
        print(f"     ticket={ticket}  deal={mt5_result.get('deal')}  price={mt5_result.get('price')}")
        print(f"     comment='{mt5_result.get('comment')}'")

        if retcode != 10009:
            print("\n     TRANSAKCJA NIE ZOSTALA OTWARTA. Test zakonczony.")
            return
    except Exception as e:
        print(f"     BLAD - wyjatek przy otwieraniu: {e}")
        return

    print(f"\n[4/4] Zamykanie transakcji ticket={ticket}...")
    try:
        close_result = api.close_transaction(
            ticket=int(ticket),
            comment="TEST_CLOSE",
        )

        if close_result is None:
            print(f"     BLAD - close_transaction zwrocilo None dla ticket={ticket}")
            print("     Sprawdz czy pozycja istnieje w MT5 i zamknij recznie.")
            return

        close_retcode = close_result.get("retcode")
        print(f"     retcode={close_retcode} -> {_retcode_desc(close_retcode)}")
        print(f"     deal={close_result.get('deal')}  price={close_result.get('price')}")
        print(f"     comment='{close_result.get('comment')}'")

        if close_retcode == 10009:
            print("\n     WYNIK: OTWARCIE i ZAMKNIECIE - OK")
        else:
            print("\n     WYNIK: OTWARCIE OK, ale ZAMKNIECIE NIEUDANE")
            print(f"     Zamknij recznie pozycje ticket={ticket} w MT5!")
    except Exception as e:
        print(f"     BLAD - wyjatek przy zamykaniu: {e}")
        print(f"     Sprawdz i zamknij recznie pozycje ticket={ticket} w MT5!")

    print("=" * 60)


if __name__ == "__main__":
    run_test()
