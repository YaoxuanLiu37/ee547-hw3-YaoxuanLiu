-- Drop in dependency order
DROP TABLE IF EXISTS stop_events;
DROP TABLE IF EXISTS line_stops;
DROP TABLE IF EXISTS trips;
DROP TABLE IF EXISTS stops;
DROP TABLE IF EXISTS lines;

-- Lines: surrogate key; keep natural name unique for lookups
CREATE TABLE lines (
  line_id      BIGSERIAL PRIMARY KEY,
  line_name    VARCHAR(50) NOT NULL UNIQUE,
  vehicle_type VARCHAR(10) NOT NULL,
  CONSTRAINT chk_vehicle_type CHECK (vehicle_type IN ('rail','bus'))
);

-- Stops: surrogate key; allow duplicate names and coordinates (no UNIQUE)
CREATE TABLE stops (
  stop_id   BIGSERIAL PRIMARY KEY,
  stop_name VARCHAR(100) NOT NULL,
  latitude  DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  CONSTRAINT chk_lat CHECK (latitude  BETWEEN -90  AND 90),
  CONSTRAINT chk_lon CHECK (longitude BETWEEN -180 AND 180)
);

-- Line_Stops: allow the same stop to appear multiple times on a line
-- Primary key is (line_id, sequence) to preserve order
CREATE TABLE line_stops (
  line_id     BIGINT    NOT NULL,
  stop_id     BIGINT    NOT NULL,
  sequence    INTEGER   NOT NULL,
  time_offset INTEGER   NOT NULL,
  CONSTRAINT pk_line_stops PRIMARY KEY (line_id, sequence),
  CONSTRAINT fk_ls_line FOREIGN KEY (line_id)
    REFERENCES lines(line_id) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_ls_stop FOREIGN KEY (stop_id)
    REFERENCES stops(stop_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  CONSTRAINT chk_sequence_pos CHECK (sequence > 0),
  CONSTRAINT chk_time_offset_nonneg CHECK (time_offset >= 0)
);

-- Trips: keep natural trip_id as PK; reference line_id
CREATE TABLE trips (
  trip_id             VARCHAR(20)  PRIMARY KEY,
  line_id             BIGINT       NOT NULL,
  scheduled_departure TIMESTAMP    NOT NULL,
  vehicle_id          VARCHAR(20)  NOT NULL,
  CONSTRAINT fk_trip_line FOREIGN KEY (line_id)
    REFERENCES lines(line_id) ON UPDATE CASCADE ON DELETE RESTRICT
);

-- Stop Events: allow duplicate events; use surrogate primary key
CREATE TABLE stop_events (
  event_id       BIGSERIAL   PRIMARY KEY,
  trip_id        VARCHAR(20) NOT NULL,
  stop_id        BIGINT      NOT NULL,
  scheduled      TIMESTAMP   NOT NULL,
  actual         TIMESTAMP   NOT NULL,
  passengers_on  INTEGER     NOT NULL DEFAULT 0,
  passengers_off INTEGER     NOT NULL DEFAULT 0,
  CONSTRAINT fk_se_trip FOREIGN KEY (trip_id)
    REFERENCES trips(trip_id) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_se_stop FOREIGN KEY (stop_id)
    REFERENCES stops(stop_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  CONSTRAINT chk_pax_on_nonneg  CHECK (passengers_on  >= 0),
  CONSTRAINT chk_pax_off_nonneg CHECK (passengers_off >= 0)
);

-- Helpful indexes
CREATE INDEX idx_lines_name          ON lines(line_name);
CREATE INDEX idx_stops_name          ON stops(stop_name);
CREATE INDEX idx_trips_line_dep      ON trips(line_id, scheduled_departure);
CREATE INDEX idx_ls_line_stop        ON line_stops(line_id, stop_id);
CREATE INDEX idx_se_trip             ON stop_events(trip_id);
CREATE INDEX idx_se_stop             ON stop_events(stop_id);
CREATE INDEX idx_se_delay            ON stop_events((actual - scheduled));
