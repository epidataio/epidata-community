/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package util

/** Represents a sort ordering direction. */
object Ordering extends Enumeration {
  type Ordering = Value
  val Ascending, Descending, Unspecified = Value
}
