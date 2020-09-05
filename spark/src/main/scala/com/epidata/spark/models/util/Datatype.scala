/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark.models.util

// scalastyle:off magic.number

/**
 * Enumeration of measurement datatypes. The value ids and names are based on
 * the persistent data format and external apis, so they should not be
 * modified.
 */
object Datatype extends Enumeration {
  type Ordering = Value

  val Double = Value(0, "double")
  val Long = Value(1, "long")
  val String = Value(2, "string")

  val DoubleArray = Value(16, "double_array")

  val Waveform = Value(32, "waveform")

  def byId(id: Int): Datatype.Value =
    values.filter(_.id == id).head

  def byName(name: String): Datatype.Value =
    values.filter(_.toString == name).head

  def isValidName(name: String): Boolean =
    values.filter(_.toString == name).nonEmpty

  def isNumeric(datatype: Datatype.Value): Boolean =
    datatype == Datatype.Double || datatype == Datatype.Long

  def isBinary(datatype: Datatype.Value): Boolean =
    datatype.id >= DoubleArray.id
}
