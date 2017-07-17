/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark.analytics

import collection.mutable.HashMap
import org.apache.spark.MeasurementValue
import java.lang.ClassCastException
import java.sql.Timestamp
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{ DoubleType, StructField, StructType }

object IMR {

  // Row additions for IMR result data.
  private case class IMRResultElement(
    I: Double,
    I_mean: Double,
    I_LCL: Double,
    I_UCL: Double,
    MR: Double,
    MR_mean: Double,
    MR_LCL: Double,
    MR_UCL: Double
  )

  /**
   * Perform IMR analysis on the supplied measurement Rows.
   * @param measurements The rows to analyze.
   * @param schema The schema definition for all rows, used to extract row fields.
   * @return A list of rows containing both the existing fields and new IMR fields,
   *         and a schema definition for these new rows.
   */
  def apply(measurements: List[Row], schema: StructType): (List[Row], StructType) = {

    if (measurements.length < 2) {
      throw new IllegalArgumentException("Not enough measurements to perform IMR.")
    }

    val measNameField = schema.fieldNames.indexOf("meas_name")
    val measValueField = schema.fieldNames.indexOf("meas_value")
    val tsField = schema.fieldNames.indexOf("ts")

    if (measurements.map(_.getString(measNameField)).distinct.length > 1) {
      throw new IllegalArgumentException("IMR measurements must all have the same meas_name.")
    }

    val sorted = measurements.sortBy(_.getAs[Timestamp](tsField).getTime)
    val values = try {
      sorted.map(_.getAs[MeasurementValue](measValueField).value.asInstanceOf[Double])
    } catch {
      case _: ClassCastException =>
        throw new IllegalArgumentException("IMR meas_value values must be Double.")
    }

    val numValues = values.length
    val I = values
    val I_mean = values.sum / values.length
    val I_LCL = I_mean - 2.66 * I_mean
    val I_UCL = I_mean + 2.66 * I_mean
    val MR_values = values.sliding(2).map(x => math.abs(x(1) - x(0))).toList
    val MR = List(Double.NaN) ++ MR_values
    val MR_mean = MR_values.sum / MR_values.length
    val MR_LCL = 0.0
    val MR_UCL = MR_mean + 3.267 * MR_mean

    val imr = I.zip(MR).map(x => IMRResultElement(
      x._1,
      I_mean,
      I_LCL,
      I_UCL,
      x._2,
      MR_mean,
      MR_LCL,
      MR_UCL
    ))

    val ret = sorted.zip(imr).map(x => Row.merge(x._1, Row.fromTuple(x._2)))
    val retSchema = StructType(schema.toSeq ++
      ScalaReflection.schemaFor[IMRResultElement].dataType.asInstanceOf[StructType].toSeq)
    (ret, retSchema)
  }

  /**
   * Apply IMR analysis to a measurement DataFrame. The DataFrame is grouped by
   * the meas_name field, and IMR is applied to each group.
   * @param df The input measurement DataFrame
   * @return A new DataFame containing IMR data in addition to the original input
   *         data.
   */
  def applyToDataFrame(df: DataFrame): DataFrame = {
    val schema = df.schema
    val measNameField = schema.fieldNames.indexOf("meas_name")
    val groupedLists = df.rdd.map(x => (x(measNameField), List(x))).reduceByKey(_ ++ _)
    val imr = groupedLists.flatMap(x => IMR.apply(x._2, schema)._1)
    val retSchema = StructType(schema.toSeq ++
      ScalaReflection.schemaFor[IMRResultElement].dataType.asInstanceOf[StructType].toSeq)
    df.sqlContext.createDataFrame(imr, retSchema)
  }

  def get() = IMR
}
