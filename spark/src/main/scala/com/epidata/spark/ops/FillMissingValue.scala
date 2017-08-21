/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark.ops

import com.epidata.spark.utils.{ ConvertUtils, DataFrameUtils }
import org.apache.spark.sql.{ SQLContext, Row, DataFrame }
import org.apache.spark.sql.functions._

class FillMissingValue(
    val meas_names: List[String],
    val method: String = "rolling",
    val s: Int = 3
) extends Transformation {
  override def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {

    val field = "meas_value"

    val size = if (s % 2 == 0) s + 1 else s

    method match {
      case "rolling" =>

        val filtered_df: List[Row] = dataFrame
          .filter(dataFrame("key2").isin(meas_names: _*) || dataFrame("key3").isin(meas_names: _*) || dataFrame("key1").isin(meas_names: _*))
          .collect().toList

        var meas_values: List[Double] = filtered_df.map(row => row.getAs[Double]("meas_value")) //TODO: implement for Long type!
        for (index <- meas_values.indices) {

          // If value is null, then substitute it!
          if (meas_values(index) == null || meas_values(index) == 0) { //TODO: it should not need for 0 here!
            var sum: Double = 0
            var count = 0

            for (i <- index - (size - 1) / 2 to index + (size + 1) / 2) {
              if (i >= 0 && i < meas_values.size && meas_values(i) != null) {
                sum = sum + meas_values(i)
                count = count + 1
              }
            }

            if (count > 0) {
              meas_values = meas_values.updated(index, sum / count)
            }
          }
        }

        val measurements = filtered_df.zipWithIndex.map {
          case (row, index) =>
            val meas_value: Option[Double] = ConvertUtils.nullToOption(meas_values(index))
            val m = DataFrameUtils.convertRowToMeasurementDB(row)
            m.meas_value match {
              case Some(x) if (x == 0) => m.copy(meas_value = meas_value)
              case None => m.copy(meas_value = meas_value)
              case _ => m.copy(meas_value = None)
            }
        }
          .filter(m => !m.meas_value.isEmpty)

        import sqlContext.implicits._
        measurements.toDF()
          .withColumn("meas_flag", lit("substituted"))
          .withColumn("meas_method", lit(method))

      case "mean" => // TODO: Remove this method

        val meanRow = dataFrame.filter(dataFrame("key2").isin(meas_names: _*) || dataFrame("key3").isin(meas_names: _*) || dataFrame("key1").isin(meas_names: _*)).select(avg(field)).first()
        val mean = if (meanRow.isNullAt(0)) null else meanRow.getDouble(0)

        dataFrame.withColumn(field, when(col(field) === null, mean).otherwise(col(field))) // replace mean for missing value
          .withColumn("meas_flag", lit("substituted")) // Adding extra column
          .withColumn("meas_method", lit("mean")) // Adding extra column

      case _ => throw new Exception("Unsupported substitution method: " + method)
    }

  }

  override def destination: String = "measurements_cleansed"
}

