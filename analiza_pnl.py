"""
Analiza Zysków i Strat (P&L) na podstawie raportu historii transakcji MT5.
"""

from bs4 import BeautifulSoup
import os
from collections import defaultdict
from datetime import datetime

HTML_PATH = r'C:\Users\ADKD\Documents\ReportHistory-52753886.html'


def parse_report(path):
    with open(path, encoding='utf-16') as f:
        content = f.read()
    soup = BeautifulSoup(content, 'html.parser')
    table = soup.find('table')
    rows = table.find_all('tr')

    trades = []
    header_found = False

    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
        if not cells:
            continue

        # Wiersz nagłówka transakcji
        if cells[0] == 'Czas' and 'Zysk' in cells:
            header_found = True
            continue

        if not header_found:
            continue

        # Pomiń wiersze podsumowań i separatory
        if len(cells) < 13:
            continue
        if cells[0].startswith('Bilans') or cells[0] == '' or cells[0] == 'Czas':
            continue

        try:
            # Kolumny: Czas_otw, Pozycja, Instrument, Typ, Scenariusz, Wolumen,
            #          Cena_otw, SL, TP, Czas_zam, Cena_zam, Prowizja, Swap, Zysk
            if len(cells) == 14:
                t = {
                    'czas_otwarcia': cells[0],
                    'pozycja': cells[1],
                    'instrument': cells[2],
                    'typ': cells[3],
                    'scenariusz': cells[4],
                    'wolumen': float(cells[5].replace(',', '.') or 0),
                    'cena_otwarcia': float(cells[6].replace(',', '.') or 0),
                    'sl': float(cells[7].replace(',', '.') or 0) if cells[7] else 0,
                    'tp': float(cells[8].replace(',', '.') or 0) if cells[8] else 0,
                    'czas_zamkniecia': cells[9],
                    'cena_zamkniecia': float(cells[10].replace(',', '.') or 0),
                    'prowizja': float(cells[11].replace(',', '.') or 0),
                    'swap': float(cells[12].replace(',', '.') or 0),
                    'zysk': float(cells[13].replace(',', '.') or 0),
                }
                trades.append(t)
        except (ValueError, IndexError):
            continue

    return trades


def analiza(trades):
    print("=" * 65)
    print("          ANALIZA ZYSKÓW I STRAT (P&L)")
    print("=" * 65)
    print(f"Łączna liczba transakcji: {len(trades)}\n")

    if not trades:
        print("Brak danych transakcji.")
        return

    total_profit = sum(t['zysk'] for t in trades)
    total_swap = sum(t['swap'] for t in trades)
    total_commission = sum(t['prowizja'] for t in trades)
    total_net = total_profit + total_swap + total_commission

    zyski = [t for t in trades if t['zysk'] > 0]
    straty = [t for t in trades if t['zysk'] < 0]
    be = [t for t in trades if t['zysk'] == 0]

    print("─" * 65)
    print("OGÓLNE PODSUMOWANIE")
    print("─" * 65)
    print(f"  Łączny zysk brutto (zamknięte):  {total_profit:>10.2f} EUR")
    print(f"  Swap:                            {total_swap:>10.2f} EUR")
    print(f"  Prowizje:                        {total_commission:>10.2f} EUR")
    print(f"  Wynik netto:                     {total_net:>10.2f} EUR")
    print()
    print(f"  Transakcje na zysk (+):          {len(zyski):>5}")
    print(f"  Transakcje na stratę (-):        {len(straty):>5}")
    print(f"  Break-even:                      {len(be):>5}")
    win_rate = len(zyski) / len(trades) * 100 if trades else 0
    print(f"  Win Rate:                        {win_rate:>9.1f}%")

    if zyski:
        avg_win = sum(t['zysk'] for t in zyski) / len(zyski)
        max_win = max(t['zysk'] for t in zyski)
        print(f"\n  Średni zysk na transakcji (+):   {avg_win:>10.2f} EUR")
        print(f"  Największy zysk:                 {max_win:>10.2f} EUR")

    if straty:
        avg_loss = sum(t['zysk'] for t in straty) / len(straty)
        max_loss = min(t['zysk'] for t in straty)
        print(f"  Średnia strata na transakcji (-): {avg_loss:>9.2f} EUR")
        print(f"  Największa strata:               {max_loss:>10.2f} EUR")

    if zyski and straty:
        avg_win = sum(t['zysk'] for t in zyski) / len(zyski)
        avg_loss = abs(sum(t['zysk'] for t in straty) / len(straty))
        rr = avg_win / avg_loss if avg_loss != 0 else 0
        print(f"  Profit Factor (avg win/loss):    {rr:>10.2f}")

    # --- Analiza wg instrumentu ---
    print("\n" + "─" * 65)
    print("WYNIKI WG INSTRUMENTU")
    print("─" * 65)
    print(f"  {'Instrument':<12} {'Transakcje':>11} {'Win':>5} {'Loss':>5} {'Win%':>7} {'Zysk EUR':>10}")
    print("  " + "-" * 55)
    by_symbol = defaultdict(list)
    for t in trades:
        by_symbol[t['instrument']].append(t)
    for sym, ts in sorted(by_symbol.items()):
        w = sum(1 for t in ts if t['zysk'] > 0)
        l = sum(1 for t in ts if t['zysk'] < 0)
        zysk = sum(t['zysk'] for t in ts)
        wr = w / len(ts) * 100
        print(f"  {sym:<12} {len(ts):>11} {w:>5} {l:>5} {wr:>6.1f}% {zysk:>10.2f}")

    # --- Analiza wg scenariusza ---
    print("\n" + "─" * 65)
    print("WYNIKI WG SCENARIUSZA")
    print("─" * 65)
    print(f"  {'Scenariusz':<18} {'Transakcje':>11} {'Win':>5} {'Loss':>5} {'Win%':>7} {'Zysk EUR':>10}")
    print("  " + "-" * 60)
    by_sc = defaultdict(list)
    for t in trades:
        by_sc[t['scenariusz']].append(t)
    for sc, ts in sorted(by_sc.items()):
        w = sum(1 for t in ts if t['zysk'] > 0)
        l = sum(1 for t in ts if t['zysk'] < 0)
        zysk = sum(t['zysk'] for t in ts)
        wr = w / len(ts) * 100
        print(f"  {sc:<18} {len(ts):>11} {w:>5} {l:>5} {wr:>6.1f}% {zysk:>10.2f}")

    # --- Analiza wg kierunku (buy/sell) ---
    print("\n" + "─" * 65)
    print("WYNIKI WG KIERUNKU")
    print("─" * 65)
    for typ in ['buy', 'sell']:
        ts = [t for t in trades if t['typ'] == typ]
        if not ts:
            continue
        w = sum(1 for t in ts if t['zysk'] > 0)
        zysk = sum(t['zysk'] for t in ts)
        wr = w / len(ts) * 100
        print(f"  {typ.upper():<8}  transakcje: {len(ts):>3}  win%: {wr:>5.1f}%  zysk: {zysk:>8.2f} EUR")

    # --- Analiza miesięczna ---
    print("\n" + "─" * 65)
    print("WYNIKI MIESIĘCZNE")
    print("─" * 65)
    print(f"  {'Miesiąc':<12} {'Transakcje':>11} {'Win':>5} {'Loss':>5} {'Win%':>7} {'Zysk EUR':>10}")
    print("  " + "-" * 55)
    by_month = defaultdict(list)
    for t in trades:
        try:
            dt = datetime.strptime(t['czas_otwarcia'], '%Y.%m.%d %H:%M:%S')
            key = dt.strftime('%Y-%m')
        except Exception:
            key = 'nieznany'
        by_month[key].append(t)
    for month, ts in sorted(by_month.items()):
        w = sum(1 for t in ts if t['zysk'] > 0)
        l = sum(1 for t in ts if t['zysk'] < 0)
        zysk = sum(t['zysk'] for t in ts)
        wr = w / len(ts) * 100
        print(f"  {month:<12} {len(ts):>11} {w:>5} {l:>5} {wr:>6.1f}% {zysk:>10.2f}")

    # --- Top 5 najlepszych i najgorszych transakcji ---
    print("\n" + "─" * 65)
    print("TOP 5 NAJLEPSZYCH TRANSAKCJI")
    print("─" * 65)
    top5 = sorted(trades, key=lambda x: x['zysk'], reverse=True)[:5]
    for t in top5:
        print(f"  {t['czas_otwarcia']}  {t['instrument']:<10} {t['typ']:<5} "
              f"{t['scenariusz']:<18} {t['zysk']:>8.2f} EUR")

    print("\n" + "─" * 65)
    print("TOP 5 NAJGORSZYCH TRANSAKCJI")
    print("─" * 65)
    bot5 = sorted(trades, key=lambda x: x['zysk'])[:5]
    for t in bot5:
        print(f"  {t['czas_otwarcia']}  {t['instrument']:<10} {t['typ']:<5} "
              f"{t['scenariusz']:<18} {t['zysk']:>8.2f} EUR")

    # --- Zapisz wyniki do CSV ---
    out_path = os.path.join(os.path.dirname(__file__), 'logs', 'analiza_pnl.csv')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write("czas_otwarcia,pozycja,instrument,typ,scenariusz,wolumen,"
                "cena_otwarcia,sl,tp,czas_zamkniecia,cena_zamkniecia,prowizja,swap,zysk\n")
        for t in trades:
            f.write(f"{t['czas_otwarcia']},{t['pozycja']},{t['instrument']},"
                    f"{t['typ']},{t['scenariusz']},{t['wolumen']},"
                    f"{t['cena_otwarcia']},{t['sl']},{t['tp']},"
                    f"{t['czas_zamkniecia']},{t['cena_zamkniecia']},"
                    f"{t['prowizja']},{t['swap']},{t['zysk']}\n")
    print(f"\n  -> Dane zapisane do: {out_path}")
    print("=" * 65)


if __name__ == '__main__':
    trades = parse_report(HTML_PATH)
    analiza(trades)
