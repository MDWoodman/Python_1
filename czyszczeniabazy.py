import sqlite3
from pathlib import Path

from config import conf as cnf


def usun_wszystkie_tabele(database_path: str | None = None) -> list[str]:
    """Usuwa wszystkie tabele uzytkownika z bazy SQLite i zwraca ich nazwy."""
    db_path = Path(database_path or cnf.DATABASE_PATH)

    if not db_path.exists():
        raise FileNotFoundError(f"Nie znaleziono pliku bazy danych: {db_path}")

    dropped_tables: list[str] = []
    conn = sqlite3.connect(str(db_path))

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            """
        )
        tables = [row[0] for row in cursor.fetchall()]

        cursor.execute("PRAGMA foreign_keys = OFF")
        for table_name in tables:
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            dropped_tables.append(table_name)
        conn.commit()
        cursor.execute("PRAGMA foreign_keys = ON")
    finally:
        conn.close()

    return dropped_tables


if __name__ == "__main__":
    removed = usun_wszystkie_tabele()
    print(f"Usunieto {len(removed)} tabel z bazy: {cnf.DATABASE_PATH}")
    if removed:
        print("Lista usunietych tabel:")
        for name in removed:
            print(f"- {name}")
