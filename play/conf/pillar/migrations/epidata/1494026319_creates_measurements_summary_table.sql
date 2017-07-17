--
-- Copyright (c) 2015-2017 EpiData, Inc.
--
-- description: creates measurements_summary table
-- authoredAt: 1494026319
-- up:

CREATE TABLE measurements_summary (
  customer TEXT,
  customer_site TEXT,
  collection TEXT,
  dataset TEXT,
  start_time TIMESTAMP,
  stop_time TIMESTAMP,
  key1 TEXT,
  key2 TEXT,
  key3 TEXT,
  meas_summary_name TEXT,
  meas_summary_value TEXT,
  meas_summary_description TEXT,
  PRIMARY KEY ((customer, customer_site, collection, dataset), start_time, stop_time, key1, key2, key3)
)

-- down:

DROP TABLE measurements_summary
