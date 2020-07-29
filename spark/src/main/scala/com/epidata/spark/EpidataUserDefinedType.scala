/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package org.apache.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ UnsafeRow, BaseGenericInternalRow, GenericInternalRow }
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A thin wrapper around Any, representing a measurement value within a Spark
 * SQL Row.
 */
@SQLUserDefinedType(udt = classOf[MeasurementValueUDT])
case class MeasurementValue(value: Any) {
  def this() = this(None)
}

/**
 * A user defined type representing a measurement value as a type union within
 * a Spark SQL row.
 */
class MeasurementValueUDT extends UserDefinedType[MeasurementValue] {

  override def sqlType: StructType = {
    StructType(Seq(
      StructField("double", DoubleType, nullable = true),
      StructField("long", LongType, nullable = true),
      StructField("string", StringType, nullable = true),
      StructField("binary", BinaryType, nullable = true)))
  }

  def serialize(obj: Any): InternalRow = {
    val row = new GenericInternalRow(4)
    obj match {
      case MeasurementValue(o: Double) => row.update(0, o)
      case MeasurementValue(o: Long) => row.update(1, o)
      case MeasurementValue(o: String) => row.update(2, UTF8String.fromString(o))
      case MeasurementValue(o: Array[Byte]) => row.update(3, o)
      case null =>
      case Some(MeasurementValue(o: Double)) => row.update(0, o)
      case Some(MeasurementValue(o: Long)) => row.update(1, o)
      case Some(MeasurementValue(o: String)) => row.update(2, UTF8String.fromString(o))
      case Some(MeasurementValue(o: Array[Byte])) => row.update(3, o)
      case None =>
    }
    row
  }

  override def serialize(obj: MeasurementValue): InternalRow = {
    val row = new GenericInternalRow(4)
    obj match {
      case MeasurementValue(o: Double) => row.update(0, o)
      case MeasurementValue(o: Long) => row.update(1, o)
      case MeasurementValue(o: String) => row.update(2, UTF8String.fromString(o))
      case MeasurementValue(o: Array[Byte]) => row.update(3, o)
      case _ =>
    }
    row
  }

  override def deserialize(datum: Any): MeasurementValue =
    datum match {
      case m: MeasurementValue => m
      case row: BaseGenericInternalRow =>
        require(
          row.numFields == 4,
          s"MeasurementValue.deserialize given row with length ${row.numFields} but requires length == 4")
        if (!row.isNullAt(0)) MeasurementValue(row.getDouble(0))
        else if (!row.isNullAt(1)) MeasurementValue(row.getLong(1))
        else if (!row.isNullAt(2)) MeasurementValue(row.getString(2))
        else if (!row.isNullAt(3)) MeasurementValue(row.getBinary(3))
        else null

      case row: UnsafeRow =>
        require(
          row.numFields == 4,
          s"MeasurementValue.deserialize given row with length ${row.numFields} but requires length == 4")
        if (!row.isNullAt(0)) MeasurementValue(row.getDouble(0))
        else if (!row.isNullAt(1)) MeasurementValue(row.getLong(1))
        else if (!row.isNullAt(2)) MeasurementValue(row.getString(2))
        else if (!row.isNullAt(3)) MeasurementValue(row.getBinary(3))
        else null
    }

  override def pyUDT: String = "epidata._private.measurement_value.MeasurementValueUDT"

  override def userClass: Class[MeasurementValue] = classOf[MeasurementValue]

  override def asNullable: MeasurementValueUDT = this
}
