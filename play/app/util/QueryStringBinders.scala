/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package util

import play.api.mvc.QueryStringBindable
import java.util.Date

/** Helper classes for parsing custom data types passed as url query parameters. */
object QueryStringBinders {

  implicit def bindableDate(implicit longBinder: QueryStringBindable[Long]) =
    new QueryStringBindable[Date] {

      override def bind(key: String, params: Map[String, Seq[String]]): Option[Either[String, Date]] =
        longBinder.bind(key, params) match {
          case Some(Right(value)) => Some(Right(new Date(value)))
          case None => None
          case _ => Some(Left("Unable to parse Date."))
        }

      def unbind(key: String, value: Date): String = longBinder.unbind(key, value.getTime)
    }

  implicit def bindableOrdering(implicit stringBinder: QueryStringBindable[String]) =
    new QueryStringBindable[Ordering.Value] {

      override def bind(key: String, params: Map[String, Seq[String]]): Option[Either[String, Ordering.Value]] =
        stringBinder.bind(key, params) match {
          case Some(Right("ascending")) => Some(Right(Ordering.Ascending))
          case Some(Right("descending")) => Some(Right(Ordering.Descending))
          case None => None
          case _ => Some(Left("Ordering must be either 'ascending' or 'descending'."))
        }

      def unbind(key: String, value: Ordering.Value): String =
        stringBinder.unbind(key, value match {
          case Ordering.Ascending => "ascending"
          case Ordering.Descending => "descending"
          case _ => "unspecified"
        })
    }

}
