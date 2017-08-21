/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.lib.models.util

import java.util.{ Date, Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import java.lang.{ Long => JLong, Double => JDouble }
import javax.xml.bind.DatatypeConverter

import com.epidata.lib.models.{ SensorMeasurementSummary, SensorMeasurementCleansed, Measurement, SensorMeasurement }
import org.json.simple.{ JSONArray, JSONObject }
import org.json.simple.parser.{ ParseException, JSONParser }

object JsonHelpers {

  def fromJson(str: String): Option[JSONObject] = {
    val parser = new JSONParser()
    try {
      val jSONObject = parser.parse(str).asInstanceOf[JSONObject]
      Some(jSONObject)
    } catch {
      case e: ParseException => None
      case _: Throwable => None
    }
  }

  def fromJsonArray(str: String): Option[JSONArray] = {
    val parser = new JSONParser()
    try {
      val jSONObject = parser.parse(str).asInstanceOf[JSONArray]
      Some(jSONObject)
    } catch {
      case e: ParseException => None
      case _: Throwable => None
    }
  }

  def toSensorMeasurement(str: String): Option[SensorMeasurement] = {
    fromJson(str) match {
      case Some(jSONObject) => Some(toSensorMeasurement(jSONObject))
      case _ => None
    }
  }

  def toSensorMeasurements(str: String): List[Option[SensorMeasurement]] = {
    fromJsonArray(str) match {
      case Some(jSONArray) => jSONArray.toArray.toList.map(
        x =>
          try {
            Some(toSensorMeasurement(x.asInstanceOf[JSONObject]))
          } catch {
            case _: Throwable => None
          }
      )
      case _ => List.empty
    }
  }

  private def putToMap(map: JMap[String, Object], field: String, value: Object): Unit = {
    if (null != value) map.put(field, value)
  }

  private def convertToJLong(l: Long): JLong = JLong.valueOf(l)
  private def convertToJDouble(d: Double): JDouble = JDouble.valueOf(d)

  private def putAnyToMap(map: JMap[String, Object], field: String, value: Any): Unit = {
    value match {
      case x: Double => putToMap(map, field, convertToJDouble(x))
      case x: Long => putToMap(map, field, convertToJLong(x))
      case x: String => putToMap(map, field, x)
      case x: Binary => putToMap(map, field, DatatypeConverter.printBase64Binary(x.backing))
      case _ =>
    }
  }

  private def putOptionToMap(map: JMap[String, Object], field: String, value: Option[Any]): Unit = {
    value match {
      case Some(x) => putToMap(map, field, x.asInstanceOf[Object])
      case _ =>
    }
  }

  private def putOptionAnyValToMap(map: JMap[String, Object], field: String, value: Option[AnyVal]): Unit = {
    value match {
      case Some(x) => putAnyToMap(map, field, x)
      case _ =>
    }
  }

  def toJLinkedHashMap(m: SensorMeasurement): JLinkedHashMap[String, Object] = {
    val map = new JLinkedHashMap[String, Object]()

    putToMap(map, "company", m.company)
    putToMap(map, "site", m.site)
    putToMap(map, "station", m.station)
    putToMap(map, "sensor", m.sensor)
    if (m.ts != null)
      putToMap(map, "ts", convertToJLong(m.ts.getTime))
    putToMap(map, "event", m.event)
    putToMap(map, "meas_name", m.meas_name)
    putOptionToMap(map, "meas_unit", m.meas_unit)
    putOptionToMap(map, "meas_status", m.meas_status)
    putOptionToMap(map, "meas_description", m.meas_description)

    putOptionToMap(map, "meas_datatype", m.meas_datatype)
    if (m.meas_value != null)
      putAnyToMap(map, "meas_value", m.meas_value)
    putOptionAnyValToMap(map, "meas_lower_limit", m.meas_lower_limit)
    putOptionAnyValToMap(map, "meas_upper_limit", m.meas_upper_limit)

    map
  }

  def toJLinkedHashMap(m: SensorMeasurementCleansed): JLinkedHashMap[String, Object] = {
    val map = new JLinkedHashMap[String, Object]()

    putToMap(map, "company", m.company)
    putToMap(map, "site", m.site)
    putToMap(map, "station", m.station)
    putToMap(map, "sensor", m.sensor)
    if (m.ts != null)
      putToMap(map, "ts", convertToJLong(m.ts.getTime))
    putToMap(map, "event", m.event)
    putToMap(map, "meas_name", m.meas_name)
    putOptionToMap(map, "meas_unit", m.meas_unit)
    putOptionToMap(map, "meas_status", m.meas_status)
    putOptionToMap(map, "meas_description", m.meas_description)
    putOptionToMap(map, "meas_flag", m.meas_flag)
    putOptionToMap(map, "meas_status", m.meas_status)

    putOptionToMap(map, "meas_datatype", m.meas_datatype)
    if (m.meas_value != null)
      putAnyToMap(map, "meas_value", m.meas_value)
    putOptionAnyValToMap(map, "meas_lower_limit", m.meas_lower_limit)
    putOptionAnyValToMap(map, "meas_upper_limit", m.meas_upper_limit)

    map
  }

  def toJLinkedHashMap(m: SensorMeasurementSummary): JLinkedHashMap[String, Object] = {
    val map = new JLinkedHashMap[String, Object]()

    putToMap(map, "company", m.company)
    putToMap(map, "site", m.site)
    putToMap(map, "station", m.station)
    putToMap(map, "sensor", m.sensor)

    if (m.start_time != null)
      putToMap(map, "start_time", convertToJLong(m.start_time.getTime))

    if (m.stop_time != null)
      putToMap(map, "stop_time", convertToJLong(m.stop_time.getTime))

    putToMap(map, "event", m.event)
    putToMap(map, "meas_name", m.meas_name)
    putToMap(map, "meas_summary_name", m.meas_summary_name)
    putToMap(map, "meas_summary_value", m.meas_summary_value)
    putToMap(map, "meas_summary_description", m.meas_summary_description)

    map
  }

  def toJson(m: SensorMeasurement): String = {
    val map = toJLinkedHashMap(m)
    JSONObject.toJSONString(map)
  }

  def toJson(sensorMeasurements: List[SensorMeasurement]): String = {
    import scala.collection.JavaConverters._
    val arr = new JLinkedList[JLinkedHashMap[String, Object]]()
    arr.addAll(
      sensorMeasurements
      .map(m => JsonHelpers.toJLinkedHashMap(m))
      .asJavaCollection
    )
    JSONArray.toJSONString(arr)
  }

  def toJson(list: JLinkedList[JLinkedHashMap[String, Object]], nextBatch: String): String = {
    val map = new JLinkedHashMap[String, Object]()
    map.put("batch", nextBatch)
    map.put("records", list)
    JSONObject.toJSONString(map)
  }

  def toSensorMeasurement(jSONObject: JSONObject): SensorMeasurement = {
    val company: String = jSONObject.get("company").asInstanceOf[String]
    val site: String = jSONObject.get("site").asInstanceOf[String]
    val station: String = jSONObject.get("station").asInstanceOf[String]
    val sensor: String = jSONObject.get("sensor").asInstanceOf[String]
    val ts: Date = new Date(jSONObject.get("ts").asInstanceOf[Long])
    val event: String = jSONObject.get("event").asInstanceOf[String]
    val meas_name: String = jSONObject.get("meas_name").asInstanceOf[String]

    val meas_unit: Option[String] = Measurement.blankToNone(jSONObject.get("meas_unit").asInstanceOf[String])
    val meas_status: Option[String] = Measurement.blankToNone(jSONObject.get("meas_status").asInstanceOf[String])
    val meas_description: Option[String] = Measurement.blankToNone(jSONObject.get("meas_description").asInstanceOf[String])

    val meas_value_jsonObject = jSONObject.get("meas_value")
    val meas_lower_limit_jsonObject = jSONObject.get("meas_lower_limit")
    val meas_upper_limit_jsonObject = jSONObject.get("meas_upper_limit")

    val datatype_str = jSONObject.get("meas_datatype") match {
      case x: String if (x != null) => Some(x)
      case _ => None
    }

    val datatype = datatype_str match {
      case Some(x) if Datatype.isValidName(x) => Datatype.byName(x)
      case _ => null
    }

    var datatype_from_value = datatype

    val meas_value =
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
            } else None

          case Datatype.String if meas_value_jsonObject != null => meas_value_jsonObject.toString
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

    val meas_lower_limit =
      try {
        datatype_from_value match {
          case Datatype.Double if meas_lower_limit_jsonObject != null => Some(meas_lower_limit_jsonObject.toString.toDouble)
          case Datatype.Long if meas_lower_limit_jsonObject != null => Some(meas_lower_limit_jsonObject.toString.toLong)
          case _ if meas_lower_limit_jsonObject != null => Some(meas_lower_limit_jsonObject.toString.toDouble)
          case _ if meas_lower_limit_jsonObject == null => None
        }
      } catch {
        case e: Exception => None
      }

    val meas_upper_limit =
      try {
        datatype_from_value match {
          case Datatype.Double if meas_upper_limit_jsonObject != null => Some(meas_upper_limit_jsonObject.toString.toDouble)
          case Datatype.Long if meas_upper_limit_jsonObject != null => Some(meas_upper_limit_jsonObject.toString.toLong)
          case _ if meas_upper_limit_jsonObject != null => Some(meas_upper_limit_jsonObject.toString.toDouble)
          case _ if meas_upper_limit_jsonObject == null => None
        }
      } catch {
        case e: Exception => None
      }

    SensorMeasurement(
      company,
      site,
      station,
      sensor,
      ts,
      event,
      meas_name,
      datatype_str,
      meas_value,
      meas_unit,
      meas_status,
      meas_lower_limit,
      meas_upper_limit,
      meas_description
    )

  }

}
