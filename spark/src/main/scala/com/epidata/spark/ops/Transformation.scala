/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark.ops

import org.apache.spark.sql.{ SQLContext, DataFrame }

trait Transformation {
  def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame
  def destination: String
}

