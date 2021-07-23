/*
* Copyright (c) 2015-2021 EpiData, Inc.
*/

package util

import play.api.mvc.QueryStringBindable
import java.util.Date

/** Helper classes for creating and parsing stream data as JSON objects. */
object JSONMessage {

  implicit def messageToJson(implicit longBinder: QueryStringBindable[Long]) = {

  }

  implicit def messageFromJson(implicit stringBinder: QueryStringBindable[String]) = {

  }

}
