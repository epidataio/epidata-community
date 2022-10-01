/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package util

import play.api.libs.json._

/** Json Format for scala's Either type. */
object JsonEitherFormat {

  implicit val eitherBigDecimalString = new Format[Either[BigDecimal, String]] {
    def reads(json: JsValue): JsResult[Either[BigDecimal, String]] = json match {
      case JsNumber(x) => JsSuccess(Left(x))
      case JsString(x) => JsSuccess(Right(x))
      case _ => JsError(__, "Could not parse measurement value.")
    }

    def writes(o: Either[BigDecimal, String]): JsValue = o match {
      case Left(x) => JsNumber(x)
      case Right(x) => JsString(x)
    }
  }

}
