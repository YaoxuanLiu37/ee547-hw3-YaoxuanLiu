# EE547 Homework 3

## 1. Schema Decisions: Natural vs Surrogate Keys

We adopted **surrogate keys** for all major tables to ensure stability and scalability of the schema.

- `lines(line_id BIGSERIAL PRIMARY KEY)` and `stops(stop_id BIGSERIAL PRIMARY KEY)` use surrogate IDs instead of natural keys like `line_name` or `stop_name`.
- The composite key `(line_id, sequence)` in `line_stops` preserves stop order per line.
- The only natural key preserved is `trips.trip_id`, which is inherently unique in the dataset.

**Reasons:**
- Surrogate keys prevent issues from duplicate or inconsistent names across CSV files.
- They make joins faster and simpler, especially for large datasets.
- They isolate schema relationships from business naming changes.
- Removing uniqueness constraints on `stops` ensures **1:1 row parity** with the CSV source files, as required by the assignment.

---

## 2. Constraints: CHECK and UNIQUE Constraints

| Type | Table | Field(s) | Description |
|------|--------|-----------|-------------|
| PRIMARY KEY | lines | line_id | Unique ID for each transit line |
| PRIMARY KEY | stops | stop_id | Unique ID for each stop |
| PRIMARY KEY | trips | trip_id | Natural trip identifier |
| PRIMARY KEY | stop_events | event_id | Unique ID per stop event |
| COMPOSITE PK | line_stops | (line_id, sequence) | Enforces unique stop order per line |
| FOREIGN KEY | line_stops.line_id | → lines(line_id) | Cascade on delete/update |
| FOREIGN KEY | line_stops.stop_id | → stops(stop_id) | Restrict on delete |
| FOREIGN KEY | trips.line_id | → lines(line_id) | Restrict on delete |
| FOREIGN KEY | stop_events.trip_id | → trips(trip_id) | Cascade on delete |
| FOREIGN KEY | stop_events.stop_id | → stops(stop_id) | Restrict on delete |
| CHECK | lines.vehicle_type | Must be either 'bus' or 'rail' |
| CHECK | stops.latitude, longitude | Must be valid coordinates (-90~90 / -180~180) |
| CHECK | line_stops.sequence, time_offset | Must be positive / non-negative |
| CHECK | stop_events.passengers_on/off | Must be ≥ 0 |
| UNIQUE | lines.line_name | Line names must be unique |

**Removed constraint:**  
The `UNIQUE(stop_name, latitude, longitude)` on `stops` was intentionally removed to keep CSV row counts perfectly aligned with database inserts (no deduplication).

**Indexes added for performance:**  
`idx_lines_name`, `idx_stops_name`, `idx_ls_line_stop`, `idx_trips_line_dep`, `idx_se_trip`, `idx_se_stop`, `idx_se_delay`.

---

## 3. Complex Query: Which Query Was Hardest and Why?

The most complex query was **Q3 - Transfer Stops**:

```sql
SELECT s.stop_name, COUNT(DISTINCT l.line_id) AS line_count
FROM stop_events se
JOIN trips t ON t.trip_id = se.trip_id
JOIN lines l ON l.line_id = t.line_id
JOIN stops s ON s.stop_id = se.stop_id
GROUP BY s.stop_name
HAVING COUNT(DISTINCT l.line_id) >= 2
ORDER BY line_count DESC, s.stop_name;
```

**Reason for complexity:**
- It involves a multi-table join and aggregation with DISTINCT counts to detect stops served by 2+ lines.
- The provided dataset contains *no overlapping stop names across different lines*, producing an empty result set.
- The empty output is correct for this dataset, but verifying correctness required careful validation (and could be misinterpreted as a logic error).

Future extensions might define “transfer” using *geographic proximity* rather than name equality, requiring spatial or coordinate clustering logic.

---

## 4. Foreign Keys: Example of Invalid Data They Prevent

Foreign keys ensure relational integrity between lines, trips, and stops.

**Example 1:** Invalid line reference  
Attempting to insert a trip referencing a non-existent line:

```sql
INSERT INTO trips (trip_id, line_id, scheduled_departure, vehicle_id)
VALUES ('T9999', 9999, '2025-10-01 06:00:00', 'V000');
-- ERROR: insert or update on table "trips" violates foreign key constraint "fk_trip_line"
```

**Example 2:** Invalid stop reference  
Inserting a stop_event pointing to an undefined stop:

```sql
INSERT INTO stop_events (trip_id, stop_id, scheduled, actual, passengers_on, passengers_off)
VALUES ('T0001', 9999, NOW(), NOW(), 3, 2);
-- ERROR: insert or update on table "stop_events" violates foreign key constraint "fk_se_stop"
```

These constraints prevent orphaned trips or stop events and guarantee data consistency across all tables.

---

## 5. When Relational: Why SQL Works Well for This Domain

Public transit data is **inherently relational**, consisting of interconnected entities:

- **Lines**, **Stops**, **Trips**, and **Events** form a normalized schema.  
- SQL’s **foreign keys** and **CHECK constraints** enforce data consistency between them.
- Multi-table **JOINs** are essential for analytics such as:
  - Transfer detection (Q3)
  - Passenger volume aggregation (Q6)
  - Delay statistics (Q8, Q9)

**Why SQL is ideal:**
- Supports strong referential integrity guarantees.
- Handles time-series and event data efficiently with indexes.
- Scales well for reporting, joining, and filtering operations.
- Natural fit for operational + analytical workloads.

With indexes applied, all ten queries execute well under the **500 ms** performance limit.

---

## Notes on Results

- Data loading matches CSV counts exactly:  
  `lines=5, stops=105, line_stops=105, trips=525, stop_events=11,025 → Total 11,765 rows`
- Queries Q1–Q10 execute successfully with valid JSON output.  
- Q3 (Transfer Stops) and Q5 (Common Stops Between Lines) return empty results **by design**, as no overlapping stops exist in the dataset.

---

*Author: Yaoxuan Liu*  


