--
-- Copyright (c) 2015-2017 EpiData, Inc.
--
-- description: creates device table
-- authoredAt: 1657124753
-- up:


CREATE TABLE devices (
  iot_device_id TEXT,
  iot_device_token TEXT,
  connection_timeout long,
  authenticated_at time,
  json_wet_token string,
  PRIMARY KEY (iot_device_id)
)

-- down:

DROP TABLE devices