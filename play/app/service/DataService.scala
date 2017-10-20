/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package service

import java.security.MessageDigest

object DataService {

  val MeasurementTopic = "measurements"
  val Delim = "_"
  private var registered_tokens: Seq[String] = List.empty

  def init(tokens: java.util.List[String]) = {
    import collection.JavaConverters._
    registered_tokens = tokens.asScala
  }

  def getMd5(inputStr: String): String = {
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    md.digest(inputStr.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }
  }

  def isValidToken(token: String): Boolean = {
    registered_tokens.contains(token)
  }

}

