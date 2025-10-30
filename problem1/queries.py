#!/usr/bin/env python3
import argparse, json, psycopg2

DESC = {
    "Q1":  "List all stops on Route 20 in order",
    "Q2":  "Trips during morning rush (07:00–09:00)",
    "Q3":  "Transfer stops (served by 2+ lines)",
    "Q4":  "Full ordered stop list for trip T0001",
    "Q5":  "Lines that serve both 'Wilshire / Veteran' and 'Le Conte / Broxton'",
    "Q6":  "Average passengers per stop-event by line",
    "Q7":  "Top 10 busiest stops (total activity)",
    "Q8":  "Delay counts by line (> 2 minutes)",
    "Q9":  "Trips with 3+ delayed stops (> 2 minutes)",
    "Q10": "Stops with above-average boardings (total_boardings)",
}

def q1():
    return ("""
    SELECT s.stop_name, ls.sequence, ls.time_offset
    FROM lines l
    JOIN line_stops ls ON ls.line_id = l.line_id
    JOIN stops s       ON s.stop_id = ls.stop_id
    WHERE l.line_name = 'Route 20'
    ORDER BY ls.sequence;
    """, ())

def q2():
    return ("""
    SELECT t.trip_id, l.line_name, t.scheduled_departure
    FROM trips t
    JOIN lines l ON l.line_id = t.line_id
    WHERE (t.scheduled_departure::time >= TIME '07:00'
       AND t.scheduled_departure::time <  TIME '09:00')
    ORDER BY t.scheduled_departure, t.trip_id;
    """, ())

def q3():
    return ("""
    SELECT
      s.stop_name,
      COUNT(DISTINCT l.line_id) AS line_count
    FROM stop_events se
    JOIN trips t ON t.trip_id = se.trip_id
    JOIN lines l ON l.line_id = t.line_id
    JOIN stops s ON s.stop_id = se.stop_id
    GROUP BY s.stop_name
    HAVING COUNT(DISTINCT l.line_id) >= 2
    ORDER BY line_count DESC, s.stop_name;
    """, ())



def q4():
    return ("""
    SELECT ls.sequence, s.stop_name, ls.time_offset
    FROM trips t
    JOIN line_stops ls ON ls.line_id = t.line_id
    JOIN stops s       ON s.stop_id = ls.stop_id
    WHERE t.trip_id = 'T0001'
    ORDER BY ls.sequence;
    """, ())

def q5():
    return ("""
    SELECT DISTINCT l.line_name
    FROM lines l
    JOIN line_stops ls ON ls.line_id = l.line_id
    WHERE ls.stop_id IN (SELECT stop_id FROM stops WHERE stop_name = 'Wilshire / Veteran')
      AND EXISTS (
        SELECT 1
        FROM line_stops ls2
        WHERE ls2.line_id = l.line_id
          AND ls2.stop_id IN (SELECT stop_id FROM stops WHERE stop_name = 'Le Conte / Broxton')
      )
    ORDER BY l.line_name;
    """, ())

def q6():
    return ("""
    SELECT l.line_name,
           AVG((se.passengers_on + se.passengers_off)::NUMERIC) AS avg_passengers
    FROM stop_events se
    JOIN trips t ON t.trip_id = se.trip_id
    JOIN lines l ON l.line_id = t.line_id
    GROUP BY l.line_name
    ORDER BY l.line_name;
    """, ())

def q7():
    return ("""
    SELECT s.stop_name,
           SUM(se.passengers_on + se.passengers_off) AS total_activity
    FROM stop_events se
    JOIN stops s ON s.stop_id = se.stop_id
    GROUP BY s.stop_name
    ORDER BY total_activity DESC, s.stop_name
    LIMIT 10;
    """, ())

def q8():
    return ("""
    SELECT l.line_name,
           COUNT(*) AS delay_count
    FROM stop_events se
    JOIN trips t ON t.trip_id = se.trip_id
    JOIN lines l ON l.line_id = t.line_id
    WHERE se.actual > se.scheduled + INTERVAL '2 minutes'
    GROUP BY l.line_name
    ORDER BY delay_count DESC, l.line_name;
    """, ())

def q9():
    return ("""
    SELECT se.trip_id,
           COUNT(*) AS delayed_stop_count
    FROM stop_events se
    WHERE se.actual > se.scheduled + INTERVAL '2 minutes'
    GROUP BY se.trip_id
    HAVING COUNT(*) >= 3
    ORDER BY delayed_stop_count DESC, se.trip_id;
    """, ())

def q10():
    return ("""
    WITH per_stop AS (
      SELECT se.stop_id, SUM(se.passengers_on) AS total_boardings
      FROM stop_events se
      GROUP BY se.stop_id
    ),
    global_avg AS (
      SELECT AVG(total_boardings)::NUMERIC AS avg_boardings FROM per_stop
    )
    SELECT s.stop_name, p.total_boardings
    FROM per_stop p
    JOIN stops s ON s.stop_id = p.stop_id
    CROSS JOIN global_avg g
    WHERE p.total_boardings > g.avg_boardings
    ORDER BY p.total_boardings DESC, s.stop_name;
    """, ())

QMAP = {"Q1": q1,"Q2": q2,"Q3": q3,"Q4": q4,"Q5": q5,"Q6": q6,"Q7": q7,"Q8": q8,"Q9": q9,"Q10": q10}

def rows_to_dicts(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def run_query(conn, qid):
    sql, params = QMAP[qid]()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        res = rows_to_dicts(cur)
    return {"query": qid, "description": DESC[qid], "results": res, "count": len(res)}

def main():
    ap = argparse.ArgumentParser(description="EE547 HW3 P1 Query Runner (surrogate keys).")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--query", choices=list(QMAP.keys()))
    g.add_argument("--all", action="store_true")
    ap.add_argument("--dbname", required=True)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", default="5432")
    ap.add_argument("--user", default="postgres")
    ap.add_argument("--password", default="")
    ap.add_argument("--format", choices=["json"], default="json")
    args = ap.parse_args()

    conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.dbname, user=args.user, password=args.password)
    try:
        if args.query:
            print(json.dumps(run_query(conn, args.query), default=str, ensure_ascii=False, indent=2))
        else:
            out = [run_query(conn, q) for q in ["Q1","Q2","Q3","Q4","Q5","Q6","Q7","Q8","Q9","Q10"]]
            print(json.dumps(out, default=str, ensure_ascii=False, indent=2))
    finally:
        conn.close()

if __name__ == "__main__":
    main()
