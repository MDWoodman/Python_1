"""
Analiza scenariuszy tradingowych pod kątem:
- Liczby wystąpień (ogólnie i wg instrumentu)
- Skuteczności (Win Rate, Profit Factor, avg zysk/strata)
- Kierunku (buy/sell)
- Zależności miesięcznych
- Rankingu scenariuszy
"""

import os
from bs4 import BeautifulSoup
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
        if cells[0] == 'Czas' and 'Zysk' in cells:
            header_found = True
            continue
        if not header_found:
            continue
        if len(cells) < 13:
            continue
        if cells[0].startswith('Bilans') or cells[0] == '' or cells[0] == 'Czas':
            continue
        try:
            if len(cells) == 14:
                czas_otw = cells[0]
                czas_zam = cells[9]
                try:
                    dt_otw = datetime.strptime(czas_otw, '%Y.%m.%d %H:%M:%S')
                    dt_zam = datetime.strptime(czas_zam, '%Y.%m.%d %H:%M:%S')
                    czas_s = (dt_zam - dt_otw).total_seconds()
                except Exception:
                    dt_otw = None
                    czas_s = None
                t = {
                    'czas_otwarcia': czas_otw,
                    'dt_otwarcia': dt_otw,
                    'czas_zamkniecia': czas_zam,
                    'czas_trwania_s': czas_s,
                    'instrument': cells[2],
                    'typ': cells[3],
                    'scenariusz': cells[4],
                    'wolumen': float(cells[5].replace(',', '.') or 0),
                    'cena_otwarcia': float(cells[6].replace(',', '.') or 0),
                    'sl': float(cells[7].replace(',', '.') or 0) if cells[7] else 0,
                    'tp': float(cells[8].replace(',', '.') or 0) if cells[8] else 0,
                    'cena_zamkniecia': float(cells[10].replace(',', '.') or 0),
                    'prowizja': float(cells[11].replace(',', '.') or 0),
                    'swap': float(cells[12].replace(',', '.') or 0),
                    'zysk': float(cells[13].replace(',', '.') or 0),
                }
                trades.append(t)
        except (ValueError, IndexError):
            continue
    return trades


def fmt_czas(s):
    if s is None:
        return 'n/d'
    h = int(s // 3600)
    m = int((s % 3600) // 60)
    return f'{h}h {m:02d}m' if h > 0 else f'{m}m'


def stats(ts):
    n = len(ts)
    zyski = [t['zysk'] for t in ts if t['zysk'] > 0]
    straty = [t['zysk'] for t in ts if t['zysk'] < 0]
    total = sum(t['zysk'] for t in ts)
    win = len(zyski)
    loss = len(straty)
    win_rate = win / n * 100 if n else 0
    avg_win = sum(zyski) / len(zyski) if zyski else 0
    avg_loss = sum(straty) / len(straty) if straty else 0
    pf = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
    czasy = [t['czas_trwania_s'] for t in ts if t['czas_trwania_s'] is not None and t['czas_trwania_s'] >= 0]
    avg_czas = sum(czasy) / len(czasy) if czasy else None
    return dict(n=n, win=win, loss=loss, be=n-win-loss, win_rate=win_rate,
                total=total, avg_win=avg_win, avg_loss=avg_loss, pf=pf,
                avg_czas=avg_czas)


def analiza_scenariuszy(trades):
    print('=' * 72)
    print('           ANALIZA SCENARIUSZY - SKUTECZNOŚĆ I WYSTĘPOWANIE')
    print('=' * 72)

    by_sc = defaultdict(list)
    for t in trades:
        sc = t['scenariusz'] if t['scenariusz'] else '(brak)'
        by_sc[sc].append(t)

    total_n = len(trades)

    # ── 1. RANKING WG ZYSKU ───────────────────────────────────────────────
    print('\n' + '─' * 72)
    print('1. RANKING SCENARIUSZY (wg zysku netto)')
    print('─' * 72)
    print(f"  {'Scen.':<18} {'N':>4} {'%occ':>6} {'Win':>4} {'Loss':>5} {'WR%':>6} "
          f"{'PF':>6} {'AvgW':>7} {'AvgL':>7} {'Zysk':>9} {'AvgCzas':>9}")
    print('  ' + '-' * 70)

    ranking = sorted(by_sc.items(), key=lambda x: sum(t['zysk'] for t in x[1]), reverse=True)
    for sc, ts in ranking:
        s = stats(ts)
        occ_pct = s['n'] / total_n * 100
        pf_str = f"{s['pf']:.2f}" if s['pf'] != float('inf') else '  inf'
        print(f"  {sc:<18} {s['n']:>4} {occ_pct:>5.1f}% {s['win']:>4} {s['loss']:>5} "
              f"{s['win_rate']:>5.1f}% {pf_str:>6} {s['avg_win']:>7.2f} "
              f"{s['avg_loss']:>7.2f} {s['total']:>9.2f} "
              f"{fmt_czas(s['avg_czas']):>9}")

    # ── 2. SCENARIUSZ x INSTRUMENT ───────────────────────────────────────
    print('\n' + '─' * 72)
    print('2. SKUTECZNOŚĆ: SCENARIUSZ x INSTRUMENT')
    print('─' * 72)

    instruments = sorted({t['instrument'] for t in trades})
    sc_list = [sc for sc, _ in ranking]

    # Nagłówek
    header = f"  {'Scen.':<10}"
    for sym in instruments:
        header += f"  {sym:>12}"
    print(header)
    print('  ' + '-' * (10 + 14 * len(instruments)))

    for sc in sc_list:
        row = f"  {sc:<10}"
        for sym in instruments:
            ts = [t for t in by_sc[sc] if t['instrument'] == sym]
            if not ts:
                row += f"  {'  -':>12}"
            else:
                s = stats(ts)
                row += f"  {s['n']:>2}tr {s['win_rate']:>4.0f}% {s['total']:>+5.1f}"
        print(row)

    # ── 3. SCENARIUSZ x KIERUNEK ─────────────────────────────────────────
    print('\n' + '─' * 72)
    print('3. SKUTECZNOŚĆ WG KIERUNKU (buy / sell)')
    print('─' * 72)
    print(f"  {'Scen.':<18} {'KIERUNEK':<6} {'N':>4} {'Win':>4} {'WR%':>6} {'AvgW':>7} {'AvgL':>7} {'Zysk':>9}")
    print('  ' + '-' * 60)
    for sc, ts in ranking:
        for typ in ['buy', 'sell']:
            sub = [t for t in ts if t['typ'] == typ]
            if not sub:
                continue
            s = stats(sub)
            print(f"  {sc:<18} {typ.upper():<6} {s['n']:>4} {s['win']:>4} "
                  f"{s['win_rate']:>5.1f}% {s['avg_win']:>7.2f} "
                  f"{s['avg_loss']:>7.2f} {s['total']:>9.2f}")

    # ── 4. SCENARIUSZ x MIESIĄC ───────────────────────────────────────────
    print('\n' + '─' * 72)
    print('4. SKUTECZNOŚĆ WG MIESIĄCA')
    print('─' * 72)

    months = sorted({t['dt_otwarcia'].strftime('%Y-%m') for t in trades if t['dt_otwarcia']})
    header = f"  {'Scen.':<18}"
    for m in months:
        header += f"  {m:>16}"
    print(header)
    print('  ' + '-' * (18 + 18 * len(months)))

    for sc in sc_list:
        row = f"  {sc:<18}"
        for m in months:
            sub = [t for t in by_sc[sc]
                   if t['dt_otwarcia'] and t['dt_otwarcia'].strftime('%Y-%m') == m]
            if not sub:
                row += f"  {'  -':>16}"
            else:
                s = stats(sub)
                row += f"  {s['n']:>2}tr {s['win_rate']:>4.0f}% {s['total']:>+6.1f}"
        print(row)

    # ── 5. WNIOSKI ────────────────────────────────────────────────────────
    print('\n' + '─' * 72)
    print('5. WNIOSKI I REKOMENDACJE')
    print('─' * 72)

    for sc, ts in ranking:
        s = stats(ts)
        issues = []
        notes = []
        if s['win_rate'] < 35:
            issues.append(f'WIN RATE KRYTYCZNY ({s["win_rate"]:.0f}%)')
        elif s['win_rate'] < 50:
            issues.append(f'win rate niski ({s["win_rate"]:.0f}%)')
        if s['total'] < 0:
            issues.append(f'STRATA NETTO ({s["total"]:.2f} EUR)')
        if s['pf'] < 1 and s['pf'] != float('inf'):
            issues.append(f'PF < 1 ({s["pf"]:.2f})')
        if s['win_rate'] >= 55 and s['total'] > 0:
            notes.append('skuteczny')
        if s['pf'] > 1.5 and s['total'] > 0:
            notes.append(f'dobry PF={s["pf"]:.2f}')

        status = '  [OK] ' if not issues else '  [!!] '
        tag = ', '.join(issues) if issues else ', '.join(notes) if notes else 'neutralny'
        print(f"{status}{sc:<18}  N={s['n']:>3}  WR={s['win_rate']:>4.0f}%  "
              f"Zysk={s['total']:>7.2f}  -> {tag}")

    # ── 6. ZAPIS DO CSV ───────────────────────────────────────────────────
    out_path = os.path.join(os.path.dirname(__file__), 'logs', 'analiza_scenariuszy.csv')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('scenariusz,instrument,typ,n,win,loss,win_rate,avg_win,avg_loss,'
                'profit_factor,zysk_total,avg_czas_min\n')
        for sc, ts in ranking:
            for sym in instruments:
                for typ in ['buy', 'sell']:
                    sub = [t for t in ts if t['instrument'] == sym and t['typ'] == typ]
                    if not sub:
                        continue
                    s = stats(sub)
                    avg_min = round(s['avg_czas'] / 60, 1) if s['avg_czas'] else ''
                    pf_val = f"{s['pf']:.4f}" if s['pf'] != float('inf') else 'inf'
                    f.write(f"{sc},{sym},{typ},{s['n']},{s['win']},{s['loss']},"
                            f"{s['win_rate']:.1f},{s['avg_win']:.4f},{s['avg_loss']:.4f},"
                            f"{pf_val},{s['total']:.4f},{avg_min}\n")

    print(f"\n  -> Dane zapisane do: {out_path}")
    print('=' * 72)


if __name__ == '__main__':
    trades = parse_report(HTML_PATH)
    analiza_scenariuszy(trades)
