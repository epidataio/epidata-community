/*
 * Copyright (c) 2015-2020 EpiData, Inc.
*/

package com.epidata.spark.models.util

import org.apache.spark.sql.Row
import java.util.Date
import java.lang.{ Double => JDouble, Long => JLong }
import scala.util.{ Success, Try }

object SQLiteTypeUtils {

  def epochForTs(ts: Date): Int =
    // Divide the timeline into epochs approximately 12 days in duration.
    (ts.getTime() / (1000L * 1000L * 1000L)).toInt

  /** Helpers for converting empty strings to None. */
  def blankToNone(string: String): Option[String] = string match {
    case "" => None
    case string => Some(string)
  }
  def optionBlankToNone(string: Option[String]): Option[String] = string match {
    case Some("") => None
    case _ => string
  }

  def stringToOption(string: String): Option[String] = string match {
    case s if (s != null) => Some(s)
    case _ => None
  }

  def getOptionDouble(row: Row, index: Int): Option[Double] = {
    if (row.isNullAt(index) && !JDouble.isNaN(row.getDouble(index)))
      Option(row.getDouble(index))
    else None
  }

  def getOptionLong(row: Row, index: Int): Option[Long] = {
    if (!row.isNullAt(index))
      Option(row.getLong(index))
    else None
  }

  def getOptionString(row: Row, index: Int): Option[String] = {
    if (!row.isNullAt(index) && row.getString(index).compareTo("") != 0)
      Option(row.getString(index))
    else None
  }

  // Todo
  def getOptionBinary(row: Row, index: Int): Option[Byte] = {
    if (!row.isNullAt(index)) {
      Option(row.getByte(index))
    } else None
    //    binaryBuf match {
    //      case null => None
    //      case _ =>
    //        val valueBytes = new Array[Byte](binaryBuf.limit - binaryBuf.position)
    //        binaryBuf.get(valueBytes)
    //        val binary = new Binary(valueBytes)
    //        Option(binary)
    //    }
  }

  def convertToLongNumber(value: String): Long = {
    value.toDouble.asInstanceOf[Number].longValue
  }

}
