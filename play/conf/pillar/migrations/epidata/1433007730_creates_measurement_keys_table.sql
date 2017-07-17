--
-- Copyright (c) 2015-2017 EpiData, Inc.
--
-- description: creates measurement_keys table
-- 'dataset' is used as the partition key for data distribution.

-- authoredAt: 1433007730
-- up:

CREATE TABLE measurements_keys (
  customer TEXT,
  customer_site TEXT,
  collection TEXT,
  dataset TEXT,
  PRIMARY KEY (dataset, customer, customer_site, collection)
)

-- down:

DROP TABLE measurement_keys
