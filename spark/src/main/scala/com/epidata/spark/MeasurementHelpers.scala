/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark

import com.datastax.driver.core.{ ProtocolVersion, Row }
import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.rdd.reader.{ RowReader, ThisRowReaderAsFactory }
import com.epidata.lib.models.{ MeasurementCleansed, MeasurementSummary }
import org.apache.spark.sql.types._
import com.datastax.spark.connector.CassandraRowMetadata

/** Helpers for reading Measurement models from cassandra Rows. */
object MeasurementHelpers {

  implicit object MeasurementReader
    extends RowReader[Measurement] with ThisRowReaderAsFactory[Measurement] {

    override def targetClass: Class[Measurement] = classOf[Measurement]

    override def neededColumns: Option[Seq[ColumnRef]] = None

    override def read(row: Row, rowMetaData: CassandraRowMetadata): Measurement = row
  }

  implicit object MeasurementCleansedReader
    extends RowReader[MeasurementCleansed] with ThisRowReaderAsFactory[MeasurementCleansed] {

    override def targetClass: Class[MeasurementCleansed] = classOf[MeasurementCleansed]

    override def neededColumns: Option[Seq[ColumnRef]] = None

    override def read(row: Row, rowMetaData: CassandraRowMetadata): MeasurementCleansed = row
  }

  implicit object MeasurementSummaryReader
    extends RowReader[MeasurementSummary] with ThisRowReaderAsFactory[MeasurementSummary] {

    override def targetClass: Class[MeasurementSummary] = classOf[MeasurementSummary]

    override def neededColumns: Option[Seq[ColumnRef]] = None

    override def read(row: Row, rowMetaData: CassandraRowMetadata): MeasurementSummary = row
  }

  implicit object MeasurementKeyReader
    extends RowReader[MeasurementKey] with ThisRowReaderAsFactory[MeasurementKey] {

    override def targetClass: Class[MeasurementKey] = classOf[MeasurementKey]

    override def neededColumns: Option[Seq[ColumnRef]] = None

    override def read(row: Row, rowMetaData: CassandraRowMetadata): MeasurementKey = MeasurementKey(
      Option(row.getString("customer")).get,
      Option(row.getString("customer_site")).get,
      Option(row.getString("collection")).get,
      Option(row.getString("dataset")).get)
  }
}
