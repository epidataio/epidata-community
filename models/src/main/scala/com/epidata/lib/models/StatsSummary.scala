/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.lib.models

case class StatsSummary(min: Float, max: Float, mean: Float, count: Long, std: Float) {
  def toJson: String = {
    val stdStr = "%.5f".format(std)
    s"""{"min": $min,"max": $max,"mean": $mean,"count": $count,"std": $stdStr}""".stripMargin
  }
}

