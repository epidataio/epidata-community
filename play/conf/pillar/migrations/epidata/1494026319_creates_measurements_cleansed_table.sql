--
-- Copyright (c) 2015-2017 EpiData, Inc.
--
-- description: creates measurements_cleansed table
-- authoredAt: 1494026319
-- up:

CREATE TABLE measurements_cleansed (
  customer TEXT,
  customer_site TEXT,
  collection TEXT,
  dataset TEXT,
  epoch INT,
  ts TIMESTAMP,
  key1 TEXT,
  key2 TEXT,
  key3 TEXT,
  meas_datatype TEXT,
  meas_value DOUBLE,
  meas_value_l BIGINT,
  meas_value_s TEXT,
  meas_value_b BLOB,
  meas_unit TEXT,
  meas_status TEXT,
  meas_flag TEXT,
  meas_method TEXT,
  meas_lower_limit DOUBLE,
  meas_lower_limit_l BIGINT,
  meas_upper_limit DOUBLE,
  meas_upper_limit_l BIGINT,
  meas_description TEXT,
  val1 TEXT,
  val2 TEXT,
  PRIMARY KEY ((customer, customer_site, collection, dataset, epoch), ts, key1, key2, key3)
)

-- down:

DROP TABLE measurements_cleansed
