--
-- Copyright (c) 2015-2017 EpiData, Inc.
--
-- description: creates device table
-- authoredAt: 1657124753
-- up:


CREATE TABLE iot_devices (
  iot_device_id TEXT,
  iot_device_token TEXT,
  connection_timeout INT,
  authenticated_at BIGINT,
  PRIMARY KEY (iot_device_id)
)


-- down:

DROP TABLE devices