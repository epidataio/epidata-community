/*
* Copyright (c) 2015-2020 EpiData, Inc.
*/

package com.epidata.lib.models.util

import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
import java.lang.{ Long => JLong, Double => JDouble }
import javax.xml.bind.DatatypeConverter

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

  def putToMap(map: JMap[String, Object], field: String, value: Object): Unit = {
    if (null != value) map.put(field, value)
  }

  def convertToJLong(l: Long): JLong = JLong.valueOf(l)
  def convertToJDouble(d: Double): JDouble = JDouble.valueOf(d)

  def putAnyToMap(map: JMap[String, Object], field: String, value: Any): Unit = {
    value match {
      case x: Double => putToMap(map, field, convertToJDouble(x))
      case x: Long => putToMap(map, field, convertToJLong(x))
      case x: String => putToMap(map, field, x)
      case x: Binary => putToMap(map, field, DatatypeConverter.printBase64Binary(x.backing))
      case _ =>
    }
  }

  def putOptionToMap(map: JMap[String, Object], field: String, value: Option[Any]): Unit = {
    value match {
      case Some(x) => putToMap(map, field, x.asInstanceOf[Object])
      case _ =>
    }
  }

  def putOptionAnyValToMap(map: JMap[String, Object], field: String, value: Option[AnyVal]): Unit = {
    value match {
      case Some(x) => putAnyToMap(map, field, x)
      case _ =>
    }
  }

  def toJson(list: JList[JLinkedHashMap[String, Object]], nextBatch: String): String = {
    val map = new JLinkedHashMap[String, Object]()
    map.put("batch", nextBatch)
    map.put("records", list)
    JSONObject.toJSONString(map)
  }

}
