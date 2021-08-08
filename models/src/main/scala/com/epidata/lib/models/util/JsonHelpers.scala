/*
* Copyright (c) 2015-2020 EpiData, Inc.
*/

package com.epidata.lib.models.util

import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
import java.lang.{ Long => JLong, Double => JDouble }
import javax.xml.bind.DatatypeConverter
import scala.collection.mutable.ListBuffer
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

  def jsonToMap(str: String): JLinkedHashMap[String, Object] = {
    val map = new JLinkedHashMap[String, Object]()

    val parser = new JSONParser()
    val jSONObject = parser.parse(str).asInstanceOf[JSONObject]
    val entryIter = jSONObject.entrySet().iterator()
    while (entryIter.hasNext()) {
      val entry = entryIter.next().asInstanceOf[java.util.Map.Entry[String, Object]]
      map.put(entry.getKey(), entry.getValue())
    }
    map
  }

  def jsonToMap(strList: List[String]): List[JLinkedHashMap[String, Object]] = {
    var mapList = ListBuffer[JLinkedHashMap[String, Object]]()
    for (str <- strList) {
      mapList :+ jsonToMap(str)
    }
    mapList.toList
  }

  def mapToJson(map: JLinkedHashMap[String, Object]): String = {
    JSONObject.toJSONString(map)
  }

  def mapToJson(mapList: List[JLinkedHashMap[String, Object]]): List[String] = {
    var jsonList = ListBuffer[String]()
    for (map <- mapList) {
      jsonList :+ JSONObject.toJSONString(map)
    }
    jsonList.toList
  }

  def messageToJson(message: Message): String = {
    val map = new java.util.HashMap[String, String]()
    map.put("key", message.key)
    map.put("value", message.value)
    JSONObject.toJSONString(map)
  }

  def jsonToMessage(json: String): Message = {
    val parser = new JSONParser()
    val map = parser.parse(json).asInstanceOf[java.util.HashMap[String, String]]
    Message(map.get("key"), map.get("value"))
  }

}

case class Message(key: String, value: String)
