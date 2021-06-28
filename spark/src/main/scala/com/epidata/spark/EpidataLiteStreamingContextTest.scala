
//package com.epidata.spark
//
//import java.sql.{DriverManager, Timestamp}
//import java.util
//import java.util.Base64
//
//import com.epidata.spark.elcTest.esc
//import javax.servlet.http.HttpSession
//
//import scala.io.{Source, StdIn}
//import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
//import org.apache.http.client.protocol.HttpClientContext
//import org.apache.http.entity.StringEntity
//import org.apache.http.impl.client.{BasicCookieStore, BasicCredentialsProvider, HttpClientBuilder, HttpClients}
//import org.apache.http.util.EntityUtils
//import org.json4s.DefaultFormats
////import org.json4s.jackson.{Json, JsonMethods}
//import org.json4s.native.Json
//import org.json4s.DefaultFormats
//
//import scala.util.parsing.json._
//
//object EpidataLiteStreamingContextTest extends App {
//  val COMPANY = "EpiData"
//  val SITE = "San_Francisco"
//  val STATION = "WSN-1"
//  val timestamp = new Timestamp(System.currentTimeMillis())
//  val current_time = timestamp.getTime()
//  //construct connection to sqlite3 database
//  val con = DriverManager.getConnection("jdbc:sqlite:data/epidata_development.db")
//  val session = con.createStatement()
//  val cleansed = "play/conf/schema//measurements_cleansed"
//  val sql1 = Source.fromFile(cleansed).getLines.mkString
//  session.executeUpdate(sql1)
//  session.execute(s"DELETE FROM  ${com.epidata.lib.models.MeasurementCleansed.DBTableName}")
//
//  //construct http post request to stream/measurements
//
//  val HOST = "127.0.0.1:9000"
//  val AUTHENTICATION_URL = "http://" + HOST + "/authenticate/app"
//  val CREATE_MEASUREMENT_URL = "http://" + HOST + "/stream/measurements"
//
//  val json = """{"accessToken":"epidata123"}"""
//  val client = HttpClients.createDefault()
//
//  val post: HttpPost = new HttpPost(AUTHENTICATION_URL)
//  post.addHeader("Content-Type", "application/json")
//  post.addHeader("Set-Cookie", "")
//  post.setEntity(new StringEntity(json))
//
//  val response: CloseableHttpResponse = client.execute(post)
//  for(e <- response.getAllHeaders){
//    print("header: ", e)
//  }
//
//  val entity = response.getEntity
//
//  val str = EntityUtils.toString(entity, "UTF-8")
//
//  println(str)
//
//  val measurement = Map(
//    "company" -> COMPANY,
//    "site" -> SITE,
//    "station" -> STATION,
//    "sensor" -> "Anemometer",
//    "ts" -> current_time,
//    "event" -> "none",
//    "meas_name" -> "Wind_Speed",
//    "meas_value" -> 14.0,
//    "meas_unit" -> "mph",
//    "meas_datatype" -> "double",
//    "meas_status" -> "PASS",
//    "meas_lower_limit" -> 0,
//    "meas_upper_limit" -> 25,
//    "meas_description" -> "")
//  //convert list of map object to json string
//  val parsed = JSON.parseFull(str).get.asInstanceOf[Map[String, Any]]//map[sessionid -> ###]
//  var lst = List(measurement)
//
//
//  print(parsed)
////  lst = parsed.asInstanceOf[Map[String, Any]] :: lst
//  val jsonBody = Json(DefaultFormats).write(lst)
//  val post2: HttpPost = new HttpPost(CREATE_MEASUREMENT_URL)
//
//  post2.addHeader("Content-Type", "application/json")
////  post2.addHeader("Authorization", "Basic " + Base64.getEncoder.encodeToString("guest:epidata123".getBytes))
//  post2.setHeader("Cookie", response.getHeaders("Set-Cookie")(0).getValue)
////  post2.setHeader("X-Auth-Token", parsed.get("sessionId").get.toString)
//  post2.setEntity(new StringEntity(jsonBody))
//
//  print(jsonBody)
//
//
////      val response2: CloseableHttpResponse = client.execute(post2)
////      val entity2 = response2.getEntity
////      val str2 = EntityUtils.toString(entity2, "UTF-8")
////      println(str2)
//
//  /*  ----- EpiDataLite Stream Test Started ----- */
//  println("\n EpiDataLite Stream Test Started")
//  val esc = new EpidataLiteStreamingContext()
//  esc.init()
//
//  // Create Transformation
//  val op1 = esc.createTransformations("Identity", List("Meas-1"), Map[String, String]())
//  println("transformation created: " + op1)
//
//  val op2 = esc.createTransformations("Identity", List("Meas-1"), Map[String, String]())
//  println("transformation created: " + op2)
//  var list = new util.ArrayList[String]()
//  list.add("Meas-1")
//  val mutableMap = new util.HashMap[String, String]
//  val op3 = esc.createTransformations("Identity", list, mutableMap)
//  println("transformation created: " + op3)
//
//  // Create Stream
//  esc.createStream("measurements_original", "measurements_intermediate", op1)
//  println("stream 1 created: " + op1)
//
//  esc.createStream("measurements_intermediate", "measurements_cleansed", op3)
//  println("stream 2 created: " + op3)
//
//  esc.testUnit()
//  print(esc.printSomething(""))
//
//  //  esc.createStream("measurements_intermediate", "measurements_cleansed", op2)
//  //  println("stream 3 created: " + op3)
//
//  // Start Stream
//  esc.startStream()
//  println("Stream started successfully")
//
//  val response2: CloseableHttpResponse = client.execute(post2)
//  val entity2 = response2.getEntity
//  val str2 = EntityUtils.toString(entity2, "UTF-8")
//  println(str2)
//
//  // check stream data in SQLite database
//  val rs = session.executeQuery(s"select * from ${com.epidata.lib.models.MeasurementCleansed.DBTableName} ")
//  print(rs)
//  System.out.println("customer = " + rs.getString("customer"))
//  System.out.println("customer_site = " + rs.getString("customer_site"))
//  System.out.println("ts = " + rs.getTimestamp("ts"))
//  System.out.println("mess_b = " + rs.getObject("meas_value"))
//  System.out.println()
//  rs.close()
//  println("Enter 'Q' to stop streaming")
//  while ((StdIn.readChar()).toLower.compare('q') != 0) {
//    println("Continuing streaming. Enter 'Q' to stop streaming.")
//  }
//
//  // Stop stream
//  esc.stopStream()
//
//  println("Stream processing stoppqed successfully.")
//
//  println("\n EpiDataLite Stream Test completed")
//  println("----------------------------------------------------")
//
//
//
//}

