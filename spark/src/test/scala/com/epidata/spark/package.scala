/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata

package object specs {
  object CassandraSchema {
    val keyspaceCreation = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }"
    val measurementsOriginalTableCreation =
      s"""
         |CREATE TABLE IF NOT EXISTS %s.${com.epidata.lib.models.Measurement.DBTableName} (
         |  customer TEXT,
         |  customer_site TEXT,
         |  collection TEXT,
         |  dataset TEXT,
         |  epoch INT,
         |  ts TIMESTAMP,
         |  key1 TEXT,
         |  key2 TEXT,
         |  key3 TEXT,
         |  meas_datatype TEXT,
         |  meas_value DOUBLE,
         |  meas_value_l BIGINT,
         |  meas_value_s TEXT,
         |  meas_value_b BLOB,
         |  meas_unit TEXT,
         |  meas_status TEXT,
         |  meas_lower_limit DOUBLE,
         |  meas_lower_limit_l BIGINT,
         |  meas_upper_limit DOUBLE,
         |  meas_upper_limit_l BIGINT,
         |  meas_description TEXT,
         |  val1 TEXT,
         |  val2 TEXT,
         |  PRIMARY KEY ((customer, customer_site, collection, dataset, epoch), ts, key1, key2, key3)
         |)
       """.stripMargin

    val measurementsCleansedTableCreation =
      s"""
            |CREATE TABLE IF NOT EXISTS %s.${com.epidata.lib.models.MeasurementCleansed.DBTableName} (
            |  customer TEXT,
            |  customer_site TEXT,
            |  collection TEXT,
            |  dataset TEXT,
            |  epoch INT,
            |  ts TIMESTAMP,
            |  key1 TEXT,
            |  key2 TEXT,
            |  key3 TEXT,
            |  meas_datatype TEXT,
            |  meas_value DOUBLE,
            |  meas_value_l BIGINT,
            |  meas_value_s TEXT,
            |  meas_value_b BLOB,
            |  meas_unit TEXT,
            |  meas_status TEXT,
            |  meas_flag TEXT,
            |  meas_method TEXT,
            |  meas_lower_limit DOUBLE,
            |  meas_lower_limit_l BIGINT,
            |  meas_upper_limit DOUBLE,
            |  meas_upper_limit_l BIGINT,
            |  meas_description TEXT,
            |  val1 TEXT,
            |  val2 TEXT,
            |  PRIMARY KEY ((customer, customer_site, collection, dataset, epoch), ts, key1, key2, key3)
            |)
          """.stripMargin

    val measurementsSummaryTableCreation =
      s"""
               |CREATE TABLE IF NOT EXISTS %s.${com.epidata.lib.models.MeasurementSummary.DBTableName} (
               |  customer TEXT,
               |  customer_site TEXT,
               |  collection TEXT,
               |  dataset TEXT,
               |  start_time TIMESTAMP,
               |  stop_time TIMESTAMP,
               |  key1 TEXT,
               |  key2 TEXT,
               |  key3 TEXT,
               |  meas_summary_name TEXT,
               |  meas_summary_value TEXT,
               |  meas_summary_description TEXT,
               |  PRIMARY KEY ((customer, customer_site, collection, dataset), start_time, stop_time, key1, key2, key3)
               |)
             """.stripMargin

    val measurementKeysTableCreation =
      s"""
         |CREATE TABLE IF NOT EXISTS %s.${com.epidata.lib.models.MeasurementsKeys.DBTableName} (
         |  customer TEXT,
         |  customer_site TEXT,
         |  collection TEXT,
         |  dataset TEXT,
         |  PRIMARY KEY (dataset, customer, customer_site, collection)
         |)
       """.stripMargin

    val userTableCreation =
      s"""
         |CREATE TABLE IF NOT EXISTS %s.users (
         |  id TEXT PRIMARY KEY,
         |  first_name TEXT,
         |  last_name TEXT,
         |  full_name TEXT,
         |  email TEXT,
         |  avatar_url TEXT,
         |  oauth2_token TEXT,
         |  oauth2_token_type TEXT,
         |  oauth2_expires_in INT,
         |  oauth2_refresh_token TEXT
         |)
       """.stripMargin
  }

  object SQLiteSchema {
    val measurementsOriginalTableCreation =
      s"""
         |CREATE TABLE IF NOT EXISTS ${com.epidata.lib.models.Measurement.DBTableName} (
         |  customer TEXT,
         |  customer_site TEXT,
         |  collection TEXT,
         |  dataset TEXT,
         |  epoch INT,
         |  ts DATETIME,
         |  key1 TEXT,
         |  key2 TEXT,
         |  key3 TEXT,
         |  meas_datatype TEXT,
         |  meas_value DOUBLE,
         |  meas_value_l BIGINT,
         |  meas_value_s TEXT,
         |  meas_value_b BLOB,
         |  meas_unit TEXT,
         |  meas_status TEXT,
         |  meas_lower_limit DOUBLE,
         |  meas_lower_limit_l BIGINT,
         |  meas_upper_limit DOUBLE,
         |  meas_upper_limit_l BIGINT,
         |  meas_description TEXT,
         |  val1 TEXT,
         |  val2 TEXT,
         |  PRIMARY KEY (customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3)
         |)
       """.stripMargin

    val measurementsCleansedTableCreation =
      s"""
            |CREATE TABLE IF NOT EXISTS ${com.epidata.lib.models.MeasurementCleansed.DBTableName} (
            |  customer TEXT,
            |  customer_site TEXT,
            |  collection TEXT,
            |  dataset TEXT,
            |  epoch INT,
            |  ts DATETIME,
            |  key1 TEXT,
            |  key2 TEXT,
            |  key3 TEXT,
            |  meas_datatype TEXT,
            |  meas_value DOUBLE,
            |  meas_value_l BIGINT,
            |  meas_value_s TEXT,
            |  meas_value_b BLOB,
            |  meas_unit TEXT,
            |  meas_status TEXT,
            |  meas_flag TEXT,
            |  meas_method TEXT,
            |  meas_lower_limit DOUBLE,
            |  meas_lower_limit_l BIGINT,
            |  meas_upper_limit DOUBLE,
            |  meas_upper_limit_l BIGINT,
            |  meas_description TEXT,
            |  val1 TEXT,
            |  val2 TEXT,
            |  PRIMARY KEY (customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3)
            |)
          """.stripMargin

    val measurementsSummaryTableCreation =
      s"""
               |CREATE TABLE IF NOT EXISTS ${com.epidata.lib.models.MeasurementSummary.DBTableName} (
               |  customer TEXT,
               |  customer_site TEXT,
               |  collection TEXT,
               |  dataset TEXT,
               |  start_time DATETIME,
               |  stop_time DATETIME,
               |  key1 TEXT,
               |  key2 TEXT,
               |  key3 TEXT,
               |  meas_summary_name TEXT,
               |  meas_summary_value TEXT,
               |  meas_summary_description TEXT,
               |  PRIMARY KEY (customer, customer_site, collection, dataset, start_time, stop_time, key1, key2, key3)
               |)
             """.stripMargin

    val measurementKeysTableCreation =
      s"""
         |CREATE TABLE IF NOT EXISTS ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (
         |  customer TEXT,
         |  customer_site TEXT,
         |  collection TEXT,
         |  dataset TEXT,
         |  PRIMARY KEY (dataset, customer, customer_site, collection)
         |)
       """.stripMargin

    val userTableCreation =
      s"""
         |CREATE TABLE IF NOT EXISTS users (
         |  providerId TEXT,
         |  userId TEXT,
         |  first_name TEXT,
         |  last_name TEXT,
         |  full_name TEXT,
         |  email TEXT,
         |  avatar_url TEXT,
         |  oauth2_token TEXT,
         |  oauth2_token_type TEXT,
         |  oauth2_expires_in INT,
         |  oauth2_refresh_token TEXT,
         |  PRIMARY KEY (providerId, userId)
         |)
       """.stripMargin
  }

}
