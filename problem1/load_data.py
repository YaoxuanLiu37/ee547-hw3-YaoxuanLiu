#!/usr/bin/env python3
import argparse
import os
import sys
import csv
import psycopg2
from psycopg2.extras import execute_batch

def run_sql_file(conn, path):
    """Execute the entire schema.sql file."""
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)

def fetch_kv(conn, sql):
    """Return a dict from a two-column SELECT (key, value)."""
    with conn.cursor() as cur:
        cur.execute(sql)
        return dict(cur.fetchall())

def main():
    parser = argparse.ArgumentParser(description="EE547 HW3 P1 Loader (surrogate keys; strict 1:1 row loading).")
    parser.add_argument("--host", required=True)
    parser.add_argument("--dbname", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--port", default="5432")
    parser.add_argument("--datadir", required=True, help="Directory containing CSV files (e.g., data/)")
    parser.add_argument("--schema", default="schema.sql", help="Path to schema.sql")
    args = parser.parse_args()

    # Resolve paths and existence checks
    schema_path = args.schema if os.path.isabs(args.schema) else os.path.join(os.getcwd(), args.schema)
    needed = ["lines.csv", "stops.csv", "line_stops.csv", "trips.csv", "stop_events.csv"]
    paths = [schema_path] + [os.path.join(args.datadir, x) for x in needed]
    for p in paths:
        if not os.path.exists(p):
            print(f"File not found: {p}", file=sys.stderr)
            sys.exit(1)

    # Connect
    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )

    try:
        conn.autocommit = False
        print(f"Connected to {args.dbname}@{args.host}")

        # Recreate schema from file
        print("Creating schema...")
        run_sql_file(conn, schema_path)
        conn.commit()
        print("Tables created: lines, stops, line_stops, trips, stop_events")

        total = 0

        # ---------- Load lines ----------
        lines_csv = os.path.join(args.datadir.rstrip("/"), "lines.csv")
        print(f"Loading {lines_csv}...", end=" ", flush=True)
        with open(os.path.join(args.datadir, "lines.csv"), newline="", encoding="utf-8") as f, conn.cursor() as cur:
            rdr = csv.DictReader(f)
            rows = [(r["line_name"], r["vehicle_type"]) for r in rdr]
            # Keep natural uniqueness on line_name; duplicates are ignored at the DB level
            execute_batch(
                cur,
                "INSERT INTO lines (line_name, vehicle_type) VALUES (%s,%s) "
                "ON CONFLICT (line_name) DO NOTHING",
                rows,
                page_size=1000,
            )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM lines")
            n = cur.fetchone()[0]
        total += n
        print(f"{n:,} rows")

        # Build mapping: line_name -> line_id
        line_map = fetch_kv(conn, "SELECT line_name, line_id FROM lines")

        # ---------- Load stops (NO de-dup; 1:1 with CSV rows) ----------
        stops_csv = os.path.join(args.datadir.rstrip("/"), "stops.csv")
        print(f"Loading {stops_csv}...", end=" ", flush=True)
        with open(os.path.join(args.datadir, "stops.csv"), newline="", encoding="utf-8") as f, conn.cursor() as cur:
            rdr = csv.DictReader(f)
            rows = [
                (r["stop_name"], float(r["latitude"]), float(r["longitude"]))
                for r in rdr
            ]
            # Insert every row; ensure your schema has NO UNIQUE(stop_name, latitude, longitude)
            execute_batch(
                cur,
                "INSERT INTO stops (stop_name, latitude, longitude) VALUES (%s,%s,%s)",
                rows,
                page_size=1000,
            )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM stops")
            n = cur.fetchone()[0]
        total += n
        print(f"{n:,} rows")

        # Use the LATEST inserted stop_id per stop_name to resolve names consistently
        stop_name_to_id = fetch_kv(conn, """
            WITH latest AS (
              SELECT stop_name, MAX(stop_id) AS stop_id
              FROM stops
              GROUP BY stop_name
            )
            SELECT stop_name, stop_id FROM latest
        """)

        # ---------- Load line_stops (translate names -> surrogate ids; keep ALL rows) ----------
        ls_csv = os.path.join(args.datadir.rstrip("/"), "line_stops.csv")
        print(f"Loading {ls_csv}...", end=" ", flush=True)
        with open(os.path.join(args.datadir, "line_stops.csv"), newline="", encoding="utf-8") as f, conn.cursor() as cur:
            rdr = csv.DictReader(f)
            rows = []
            for r in rdr:
                ln = r["line_name"]
                sn = r["stop_name"]
                if ln not in line_map or sn not in stop_name_to_id:
                    # Skip rows that reference unknown line or stop name
                    continue
                rows.append((
                    line_map[ln],
                    stop_name_to_id[sn],
                    int(r["sequence"]),
                    int(r["time_offset"])
                ))
            if rows:
                # IMPORTANT: no ON CONFLICT; allow duplicates to match CSV exactly
                execute_batch(
                    cur,
                    "INSERT INTO line_stops (line_id, stop_id, sequence, time_offset) VALUES (%s,%s,%s,%s)",
                    rows,
                    page_size=1000,
                )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM line_stops")
            n = cur.fetchone()[0]
        total += n
        print(f"{n:,} rows")

        # ---------- Load trips (translate line_name -> line_id; keep ALL unique trip_id) ----------
        trips_csv = os.path.join(args.datadir.rstrip("/"), "trips.csv")
        print(f"Loading {trips_csv}...", end=" ", flush=True)
        with open(os.path.join(args.datadir, "trips.csv"), newline="", encoding="utf-8") as f, conn.cursor() as cur:
            rdr = csv.DictReader(f)
            rows = []
            for r in rdr:
                ln = r["line_name"]
                if ln not in line_map:
                    continue
                rows.append((
                    r["trip_id"],
                    line_map[ln],
                    r["scheduled_departure"],
                    r["vehicle_id"]
                ))
            if rows:
                execute_batch(
                    cur,
                    "INSERT INTO trips (trip_id, line_id, scheduled_departure, vehicle_id) VALUES (%s,%s,%s,%s) "
                    "ON CONFLICT (trip_id) DO NOTHING",
                    rows,
                    page_size=1000,
                )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM trips")
            n = cur.fetchone()[0]
        total += n
        print(f"{n:,} rows")

        # ---------- Load stop_events (translate stop_name -> latest stop_id; keep ALL rows) ----------
        se_csv = os.path.join(args.datadir.rstrip("/"), "stop_events.csv")
        print(f"Loading {se_csv}...", end=" ", flush=True)
        with open(os.path.join(args.datadir, "stop_events.csv"), newline="", encoding="utf-8") as f, conn.cursor() as cur:
            rdr = csv.DictReader(f)
            rows = []
            for r in rdr:
                sn = r["stop_name"]
                if sn not in stop_name_to_id:
                    continue
                rows.append((
                    r["trip_id"],
                    stop_name_to_id[sn],
                    r["scheduled"],
                    r["actual"],
                    int(r["passengers_on"]),
                    int(r["passengers_off"])
                ))
            if rows:
                # IMPORTANT: no ON CONFLICT; allow true row parity with CSV
                execute_batch(
                    cur,
                    "INSERT INTO stop_events (trip_id, stop_id, scheduled, actual, passengers_on, passengers_off) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    rows,
                    page_size=2000,
                )
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM stop_events")
            n = cur.fetchone()[0]
        total += n
        print(f"{n:,} rows")

        # ---------- Summary ----------
        print(f"\nTotal: {total:,} rows loaded")

    except Exception as e:
        conn.rollback()
        print(f"Load failed: {e}", file=sys.stderr)
        sys.exit(2)
    finally:
        conn.close()

if __name__ == "__main__":
    main()

