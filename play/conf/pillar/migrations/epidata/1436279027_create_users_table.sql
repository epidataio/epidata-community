--
-- Copyright (c) 2015-2017 EpiData, Inc.
--
-- description: creates users table

-- authoredAt: 1436279027
-- up:

CREATE TABLE users (
  id TEXT PRIMARY KEY,
  first_name TEXT,
  last_name TEXT,
  full_name TEXT,
  email TEXT,
  avatar_url TEXT,
  oauth2_token TEXT,
  oauth2_token_type TEXT,
  oauth2_expires_in INT,
  oauth2_refresh_token TEXT
)

-- down:

DROP TABLE users
