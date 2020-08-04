package com.epidata.lib.models.util

import com.datastax.driver.core.Row
import java.util.Date
import java.lang.{ Double => JDouble, Long => JLong }

import scala.util.{ Success, Try }
import java.sql.ResultSet

object TypeUtils {

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

  def getOptionDouble(row: Row, field: String): Option[Double] = {
    if (!row.isNull(field) && !JDouble.isNaN(row.getDouble(field)))
      Option(row.getDouble(field))
    else None
  }

  def getOptionDouble(row: ResultSet, field: String): Option[Double] = {
    val temp = row.getDouble(field)
    if (!row.wasNull && !JDouble.isNaN(temp))
      Option(row.getDouble(field))
    else None
  }

  def getOptionLong(row: Row, field: String): Option[Long] = {
    if (!row.isNull(field))
      Option(row.getLong(field))
    else None
  }

  def getOptionLong(row: ResultSet, field: String): Option[Long] = {
    val temp = row.getLong(field)
    if (!row.wasNull())
      Option(temp)
    else None
  }

  def getOptionString(row: Row, field: String): Option[String] = {
    if (!row.isNull(field) && row.getString(field).compareTo("") != 0)
      Option(row.getString(field))
    else None
  }

  def getOptionString(row: ResultSet, field: String): Option[String] = {
    val temp = row.getString(field)
    if (!row.wasNull() && temp.compareTo("") != 0)
      Option(temp)
    else None
  }

  def getOptionBinary(row: Row, field: String): Option[Binary] = {
    val binaryBuf = row.getBytes(field)
    binaryBuf match {
      case null => None
      case _ =>
        val valueBytes = new Array[Byte](binaryBuf.limit - binaryBuf.position)
        binaryBuf.get(valueBytes)
        val binary = new Binary(valueBytes)
        Option(binary)
    }
  }

  def getOptionBinary(row: ResultSet, field: String): Option[Binary] = {
    val binaryBuf = row.getBytes(field)
    binaryBuf match {
      case null => None
      case _ =>

        // Not sure if there is an equivalent buffer in sqlite
        // val valueBytes = new Array[Byte](binaryBuf.limit - binaryBuf.position)
        // binaryBuf.get(valueBytes)
        val binary = new Binary(binaryBuf)
        Option(binary)
    }
  }

  def convertToLongNumber(value: String): Long = {
    value.toDouble.asInstanceOf[Number].longValue
  }

  def getMeasValues(datatype: Datatype.Value, meas_value_jsonObject: Any, meas_lower_limit_jsonObject: Any, meas_upper_limit_jsonObject: Any): (Any, Option[AnyVal], Option[AnyVal], Boolean) = {
    var datatype_from_value = datatype
    var isInvalid = false
    var meas_value =
      try {
        datatype match {
          case Datatype.Double if meas_value_jsonObject != null =>
            if (meas_value_jsonObject.isInstanceOf[String]) {
              datatype_from_value = Datatype.String
              meas_value_jsonObject.toString
            } else if (meas_value_jsonObject.isInstanceOf[java.lang.Long] || meas_value_jsonObject.isInstanceOf[java.lang.Double]) {
              datatype_from_value = Datatype.Double
              meas_value_jsonObject.toString.toDouble
            } else None

          case Datatype.Long if meas_value_jsonObject != null =>
            if (meas_value_jsonObject.isInstanceOf[String]) {
              datatype_from_value = Datatype.String
              meas_value_jsonObject.toString
            } else if (meas_value_jsonObject.isInstanceOf[java.lang.Long]) {
              datatype_from_value = Datatype.Long
              meas_value_jsonObject.toString.toLong
            } else if (meas_value_jsonObject.isInstanceOf[java.lang.Double]) {
              datatype_from_value = Datatype.Double
              meas_value_jsonObject.toString.toDouble
            } else {
              datatype_from_value = Datatype.String
              None
            }

          case Datatype.String if meas_value_jsonObject != null =>
            if (meas_value_jsonObject.isInstanceOf[String]) {
              datatype_from_value = Datatype.String
              meas_value_jsonObject.toString
            } else if (meas_value_jsonObject.isInstanceOf[java.lang.Long]) {
              datatype_from_value = Datatype.Long
              meas_value_jsonObject.toString.toLong
            } else if (meas_value_jsonObject.isInstanceOf[java.lang.Double]) {
              datatype_from_value = Datatype.Double
              meas_value_jsonObject.toString.toDouble
            } else None

          case Datatype.DoubleArray | Datatype.Waveform if meas_value_jsonObject != null => Binary.fromBase64(datatype, meas_value_jsonObject.asInstanceOf[String])
          case _ if meas_value_jsonObject == null => None
          case _ if meas_value_jsonObject != null =>
            if (meas_value_jsonObject.isInstanceOf[String]) {
              datatype_from_value = Datatype.String
              meas_value_jsonObject.toString
            } else if (meas_value_jsonObject.isInstanceOf[java.lang.Long] || meas_value_jsonObject.isInstanceOf[java.lang.Double]) {
              datatype_from_value = Datatype.Double
              meas_value_jsonObject.toString.toDouble
            } else None
        }
      } catch {
        case _: Throwable => {
          datatype_from_value = datatype
          None
        }
      }

    if (meas_value == None)
      datatype_from_value = null

    val meas_lower_limit =
      try {
        datatype_from_value match {
          case Datatype.Long if meas_lower_limit_jsonObject != null && meas_lower_limit_jsonObject.isInstanceOf[java.lang.Long] => Some(meas_lower_limit_jsonObject.toString.toLong)
          case Datatype.Long if meas_lower_limit_jsonObject != null && meas_lower_limit_jsonObject.isInstanceOf[java.lang.Double] =>
            datatype_from_value = Datatype.Double
            if (meas_value != null) {
              meas_value = meas_value.toString.toDouble
            }
            Some(meas_lower_limit_jsonObject.toString.toDouble)
          case _ if meas_lower_limit_jsonObject != null && !meas_lower_limit_jsonObject.isInstanceOf[java.lang.Long] && !meas_lower_limit_jsonObject.isInstanceOf[java.lang.Double] =>
            isInvalid = true
            None
          case _ if meas_lower_limit_jsonObject != null && (meas_lower_limit_jsonObject.isInstanceOf[java.lang.Long] || meas_lower_limit_jsonObject.isInstanceOf[java.lang.Double]) => Some(meas_lower_limit_jsonObject.toString.toDouble)
          case _ if meas_lower_limit_jsonObject == null => None
        }
      } catch {
        case e: Exception => None
      }

    val meas_upper_limit =
      try {
        datatype_from_value match {
          case Datatype.Long if meas_upper_limit_jsonObject != null && meas_upper_limit_jsonObject.isInstanceOf[java.lang.Long] => Some(meas_upper_limit_jsonObject.toString.toLong)
          case _ if meas_upper_limit_jsonObject != null && !meas_upper_limit_jsonObject.isInstanceOf[java.lang.Long] && !meas_upper_limit_jsonObject.isInstanceOf[java.lang.Double] =>
            isInvalid = true
            None
          case _ if meas_upper_limit_jsonObject != null && (meas_upper_limit_jsonObject.isInstanceOf[java.lang.Long] || meas_upper_limit_jsonObject.isInstanceOf[java.lang.Double]) => Some(meas_upper_limit_jsonObject.toString.toDouble)
          case _ if meas_upper_limit_jsonObject == null => None
        }
      } catch {
        case e: Exception => None
      }

    (meas_value, meas_lower_limit, meas_upper_limit, isInvalid)
  }

}
