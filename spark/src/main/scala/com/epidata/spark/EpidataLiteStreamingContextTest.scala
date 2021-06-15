package com.epidata.spark

import java.sql.DriverManager
import java.util

import com.epidata.spark.elcTest.esc

import scala.io.{Source, StdIn}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object EpidataLiteStreamingContextTest extends App {
 //construct connection to sqlite3 database
  val con = DriverManager.getConnection("jdbc:sqlite:data/epidata_development.db")
  val session = con.createStatement()
  val cleansed = "play/conf/schema//measurements_cleansed"
  val sql1 = Source.fromFile(cleansed).getLines.mkString
  session.executeUpdate(sql1)
  session.execute(s"DELETE FROM  ${com.epidata.lib.models.MeasurementCleansed.DBTableName}")

 //construct http post request to stream/measurements

  val HOST = "127.0.0.1:9443"
  val AUTHENTICATION_URL = "https://" + HOST + "/authenticate/app"
  val CREATE_MEASUREMENT_URL = "https://" + HOST + "/stream/measurements"

  val json = "{accessToken : epidata123}"
  val client = HttpClients.createDefault()
  val post:HttpPost = new HttpPost(AUTHENTICATION_URL)

  post.addHeader("Content-Type", "application/json")
  post.addHeader("Set-Cookie", "epidata")
  post.setEntity(new StringEntity(json))

  val response:CloseableHttpResponse = client.execute(post)
  val entity = response.getEntity
  val str = EntityUtils.toString(entity,"UTF-8")
  println(str)




  /*  ----- EpiDataLite Stream Test Started ----- */
//  println("\n EpiDataLite Stream Test Started")
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
//  // check stream data in SQLite database
//
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

}
