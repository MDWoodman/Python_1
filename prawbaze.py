from __future__ import annotations

import argparse
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from config import conf as cnf


TIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y.%m.%d %H:%M:%S",
    "%Y.%m.%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
)


@dataclass
class TableFixStats:
    table: str
    scanned: int = 0
    updated: int = 0
    skipped: int = 0
    duplicates_deleted: int = 0


def _parse_epoch_ms(value: object) -> int | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        numeric = int(value)
        return numeric if numeric > 10_000_000_000 else numeric * 1000

    text = str(value).strip()
    if not text:
        return None

    if text.isdigit():
        numeric = int(text)
        return numeric if numeric > 10_000_000_000 else numeric * 1000

    for time_format in TIME_FORMATS:
        try:
            parsed = datetime.strptime(text, time_format)
            return int(parsed.timestamp() * 1000)
        except ValueError:
            continue

    return None


def _epoch_ms_to_utc_timestr(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S")


def _backup_database(db_path: Path) -> Path:
    backup_path = db_path.with_name(f"{db_path.stem}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}{db_path.suffix}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def _get_candle_tables(cursor: sqlite3.Cursor) -> list[str]:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'candles_%' ORDER BY name")
    return [row[0] for row in cursor.fetchall()]


def _ensure_timestr_column(cursor: sqlite3.Cursor, table: str) -> None:
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cursor.fetchall()]
    if "timestr" not in columns:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN timestr TEXT")


def _fix_single_table(conn: sqlite3.Connection, table: str, dry_run: bool) -> TableFixStats:
    cursor = conn.cursor()
    _ensure_timestr_column(cursor, table)

    stats = TableFixStats(table=table)

    cursor.execute(f"SELECT rowid, time, timestr FROM {table} ORDER BY rowid")
    rows = cursor.fetchall()

    for rowid, raw_time, raw_timestr in rows:
        stats.scanned += 1

        parsed_time_ms = _parse_epoch_ms(raw_time)
        parsed_timestr_ms = _parse_epoch_ms(raw_timestr)

        canonical_ms = parsed_time_ms if parsed_time_ms is not None else parsed_timestr_ms
        if canonical_ms is None:
            stats.skipped += 1
            continue

        canonical_timestr = _epoch_ms_to_utc_timestr(canonical_ms)

        current_time_norm = parsed_time_ms
        current_timestr_norm = str(raw_timestr).strip() if raw_timestr is not None else None

        needs_update = (
            current_time_norm != canonical_ms
            or current_timestr_norm != canonical_timestr
            or isinstance(raw_time, str)
        )

        if not needs_update:
            continue

        if dry_run:
            stats.updated += 1
            continue

        try:
            cursor.execute(
                f"UPDATE {table} SET time = ?, timestr = ? WHERE rowid = ?",
                (canonical_ms, canonical_timestr, rowid),
            )
            stats.updated += 1
        except sqlite3.IntegrityError:
            # If unique index on time exists and target time already exists, remove duplicate row.
            cursor.execute(f"DELETE FROM {table} WHERE rowid = ?", (rowid,))
            stats.duplicates_deleted += 1

    return stats


def fix_candles_time_columns(db_path: Path, dry_run: bool, backup: bool) -> list[TableFixStats]:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    if backup and not dry_run:
        backup_path = _backup_database(db_path)
        print(f"Backup created: {backup_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()
        tables = _get_candle_tables(cursor)
        if not tables:
            print("No candles_* tables found.")
            return []

        stats_list: list[TableFixStats] = []
        for table in tables:
            stats = _fix_single_table(conn, table, dry_run=dry_run)
            stats_list.append(stats)

        if dry_run:
            conn.rollback()
        else:
            conn.commit()

        return stats_list
    finally:
        conn.close()


def _print_summary(stats_list: list[TableFixStats], dry_run: bool) -> None:
    mode = "DRY RUN" if dry_run else "APPLIED"
    print(f"\n=== TIME/TIMESTR FIX SUMMARY ({mode}) ===")

    total_scanned = 0
    total_updated = 0
    total_skipped = 0
    total_deleted = 0

    for stats in stats_list:
        total_scanned += stats.scanned
        total_updated += stats.updated
        total_skipped += stats.skipped
        total_deleted += stats.duplicates_deleted

        print(
            f"{stats.table}: scanned={stats.scanned}, updated={stats.updated}, "
            f"skipped={stats.skipped}, duplicates_deleted={stats.duplicates_deleted}"
        )

    print(
        f"TOTAL: scanned={total_scanned}, updated={total_updated}, "
        f"skipped={total_skipped}, duplicates_deleted={total_deleted}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Naprawia kolumny time i timestr w tabelach candles_* w SQLite."
    )
    parser.add_argument(
        "--db-path",
        default=cnf.DATABASE_PATH,
        help="Sciezka do bazy SQLite (domyslnie z config.conf.DATABASE_PATH)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Nie zapisuje zmian, tylko pokazuje ile rekordow zostaloby poprawionych.",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Nie tworz kopii zapasowej bazy przed zapisem zmian.",
    )

    args = parser.parse_args()
    db_path = Path(args.db_path)

    stats_list = fix_candles_time_columns(
        db_path=db_path,
        dry_run=args.dry_run,
        backup=not args.no_backup,
    )
    _print_summary(stats_list, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
