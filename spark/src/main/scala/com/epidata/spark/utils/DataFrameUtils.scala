/*
 * Copyright (c) 2015-2020 EpiData, Inc.
*/

package com.epidata.spark.utils

import java.sql.Timestamp

import com.epidata.lib.models.{ MeasurementSummary }
import com.epidata.spark.models.MeasurementDB
import org.apache.spark.sql.Row

object DataFrameUtils {
  def convertRowToMeasurementDB(row: Row): MeasurementDB = {
    val customer: String = row.getAs[String]("customer")
    val customer_site: String = row.getAs[String]("customer_site")
    val collection: String = row.getAs[String]("collection")
    val dataset: String = row.getAs[String]("dataset")
    val epoch: Int = row.getAs[Int]("epoch")
    val ts: Timestamp = row.getAs[Timestamp]("ts")
    val key1: String = row.getAs[String]("key1")
    val key2: String = row.getAs[String]("key2")
    val key3: String = row.getAs[String]("key3")
    val meas_datatype: String = row.getAs[String]("meas_datatype")
    val meas_value: Option[Double] = ConvertUtils.nullToOption(row.getAs[Double]("meas_value"))
    val meas_value_l: Option[Long] = ConvertUtils.nullToOption(row.getAs[Long]("meas_value_l"))
    val meas_value_s: Option[String] = ConvertUtils.nullToOption(row.getAs[String]("meas_value_s"))
    val meas_value_b: Option[Array[Byte]] = ConvertUtils.nullToOption(row.getAs[Array[Byte]]("meas_value_b"))
    val meas_unit: Option[String] = ConvertUtils.nullToOption(row.getAs[String]("meas_unit"))
    val meas_status: Option[String] = ConvertUtils.nullToOption(row.getAs[String]("meas_status"))
    val meas_lower_limit: Option[Double] = ConvertUtils.nullToOption(row.getAs[Double]("meas_lower_limit"))
    val meas_lower_limit_l: Option[Long] = ConvertUtils.nullToOption(row.getAs[Long]("meas_lower_limit_l"))
    val meas_upper_limit: Option[Double] = ConvertUtils.nullToOption(row.getAs[Double]("meas_upper_limit"))
    val meas_upper_limit_l: Option[Long] = ConvertUtils.nullToOption(row.getAs[Long]("meas_upper_limit_l"))
    val meas_description: Option[String] = ConvertUtils.nullToOption(row.getAs[String]("meas_description"))
    val val1: Option[String] = ConvertUtils.nullToOption(row.getAs[String]("val1"))
    val val2: Option[String] = ConvertUtils.nullToOption(row.getAs[String]("val2"))

    MeasurementDB(
      customer,
      customer_site,
      collection,
      dataset,
      epoch,
      ts,
      key1,
      key2,
      key3,
      Some(meas_datatype),
      meas_value,
      meas_value_l,
      meas_value_s,
      meas_value_b,
      meas_unit,
      meas_status,
      meas_lower_limit,
      meas_lower_limit_l,
      meas_upper_limit,
      meas_upper_limit_l,
      meas_description,
      val1,
      val2)

  }

  def whereStatementForTable(tableName: String): String = {
    tableName match {

      case MeasurementSummary.DBTableName =>
        """#customer = ? AND
          					 #customer_site = ? AND
          					 #collection = ? AND
          					 #dataset = ? AND
          					 #start_time >= ? AND
          					 #start_time < ?""".stripMargin('#')
      case _ =>
        """#customer = ? AND
          					 #customer_site = ? AND
          					 #collection = ? AND
          					 #dataset = ? AND
          					 #epoch = ? AND
          					 #ts >= ? AND
          					 #ts < ?""".stripMargin('#')
    }
  }

  def whereStatementForTable(tableName: String, parts: List[Any]): String = {
    tableName match {
      case MeasurementSummary.DBTableName =>
        s"""#customer = ${parts.lift(0)} AND
          					 #customer_site = ${parts.lift(1)} AND
          					 #collection = ${parts.lift(2)} AND
          					 #dataset = ${parts.lift(3)} AND
          					 #start_time >= ${parts.lift(4)} AND
          					 #start_time < ${parts.lift(5)}""".stripMargin('#')
      case _ =>
        s"""#customer = ${parts.lift(0)} AND
          					 #customer_site = ${parts.lift(1)} AND
          					 #collection = ${parts.lift(2)} AND
          					 #dataset = ${parts.lift(3)} AND
          					 #epoch = ${parts.lift(4)} AND
          					 #ts >= ${parts.lift(5)} AND
          					 #ts < ${parts.lift(6)}""".stripMargin('#')
    }
  }
}
