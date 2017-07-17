/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import com.datastax.driver.core.ResultSet
import com.epidata.lib.models.{ SensorMeasurement => Model, SensorMeasurementCleansed, SensorMeasurementSummary, MeasurementSummary }
import com.epidata.lib.models.util.Binary
import com.epidata.lib.models.util.Datatype
import java.util.Date
import play.api.Logger
import play.api.libs.functional.syntax._
import play.api.libs.json._
import _root_.util.Ordering
import com.epidata.lib.models.SensorMeasurement._
import scala.collection.convert.WrapAsScala
import scala.language.implicitConversions

object SensorMeasurement {

  import com.epidata.lib.models.SensorMeasurement._
  import com.epidata.lib.models.MeasurementSummary._

  /**
   * Insert a Double sensor measurement into the database.
   * @param sensorMeasurement The SensorMeasurement to insert.
   */
  def insert(sensorMeasurement: Model): Unit =
    Measurement.insert(sensorMeasurement)

  def insertList(sensorMeasurementList: List[Model]): Unit = Measurement.bulkInsert(sensorMeasurementList)

  /**
   * Find sensor measurements in the database matching the specified parameters.
   * @param company
   * @param site
   * @param station
   * @param sensor
   * @param beginTime Beginning of query time interval, inclusive
   * @param endTime End of query time interval, exclusive
   * @param ordering Timestamp ordering of results, if specified.
   */
  @Deprecated
  def find(
    company: String,
    site: String,
    station: String,
    sensor: String,
    beginTime: Date,
    endTime: Date,
    ordering: Ordering.Value,
    tableName: String = com.epidata.lib.models.Measurement.DBTableName
  ): List[Model] = Measurement.find(company, site, station, sensor, beginTime, endTime, ordering, tableName)
    .map(measurementToSensorMeasurement)

  def query(
    company: String,
    site: String,
    station: String,
    sensor: String,
    beginTime: Date,
    endTime: Date,
    size: Int,
    batch: String,
    ordering: Ordering.Value,
    tableName: String = com.epidata.lib.models.Measurement.DBTableName
  ): JsObject = {

    // Get the data from Cassandra
    val rs: ResultSet = Measurement.query(company, site, station, sensor, beginTime, endTime, ordering, tableName, size, batch)

    // Get the next page info
    val nextPage = rs.getExecutionInfo().getPagingState()
    val nextBatch = if (nextPage == null) "" else nextPage.toString

    // only return the available ones by not fetching.
    val rows = 1.to(rs.getAvailableWithoutFetching()).map(_ => rs.one())

    // Convert the model to SensorMeasurements

    val records = tableName match {
      case MeasurementSummary.DBTableName =>
        val measurements = rows.map(MeasurementSummary.rowToMeasurementSummary).toList.map(measurementSummaryToSensorMeasurementSummary)
        SensorMeasurement.toMeasurementSummaryJson(measurements)
      case com.epidata.lib.models.MeasurementCleansed.DBTableName =>
        val measurements = rows.map(com.epidata.lib.models.MeasurementCleansed.rowToMeasurementCleansed).toList.map(measurementCleansedToSensorMeasurementCleansed)
        SensorMeasurement.toMeasurementCleansedJson(measurements)
      case com.epidata.lib.models.Measurement.DBTableName =>
        val measurements = rows.map(com.epidata.lib.models.Measurement.rowToMeasurement).toList.map(measurementToSensorMeasurement)
        SensorMeasurement.toJson(measurements)
    }

    // Return the json object
    Json.obj("batch" -> nextBatch, "records" -> records)
  }

  /** Parse the json representation of a sensor measurement to a SensorMeasurement. */
  def parseJson(json: JsValue): JsResult[Model] = {
    import JsonFormats._
    json.validate[EncodedSensorMeasurement] match {
      case error: JsError => error

      case JsSuccess(encoded: EncodedSensorMeasurement, _) if !Datatype.isValidName(encoded.meas_datatype) =>
        JsError(__, s"Could not parse datatype ${encoded.meas_datatype}")

      case JsSuccess(encoded: EncodedSensorMeasurement, _) =>
        val datatype = Datatype.byName(encoded.meas_datatype)

        if (Datatype.isNumeric(datatype) && encoded.meas_value.isRight) {
          JsError(__, "Invalid meas_value for meas_datatype")

        } else if (!Datatype.isNumeric(datatype) && encoded.meas_value.isLeft) {
          JsError(__, "Invalid meas_value for meas_datatype")

        } else if (datatype == Datatype.String && encoded.meas_unit.nonEmpty) {
          JsError(__, "String measurement may not contain a meas_unit")

        } else if (datatype != Datatype.String && encoded.meas_unit.isEmpty) {
          JsError(__, "Measurement must contain a meas_unit")

        } else {
          JsSuccess(encoded)
        }

    }
  }

  /** Parse the json representation of a list of sensor measurements to a list of SensorMeasurement. */
  def parseJsonList(json: JsValue): JsResult[List[Model]] = {
    import JsonFormats._
    json.validate[List[EncodedSensorMeasurement]] match {
      case error: JsError => error

      case JsSuccess(encodedList: List[EncodedSensorMeasurement], _) if encodedList.exists(encoded =>
        !Datatype.isValidName(encoded.meas_datatype)) => JsError(__, s"Could not parse datatype in encoded list")

      case JsSuccess(encodedList: List[EncodedSensorMeasurement], _) =>
        encodedList.foreach { encoded =>
          val datatype = Datatype.byName(encoded.meas_datatype)

          if (Datatype.isNumeric(datatype) && encoded.meas_value.isRight) {
            JsError(__, "Invalid meas_value for meas_datatype")

          } else if (!Datatype.isNumeric(datatype) && encoded.meas_value.isLeft) {
            JsError(__, "Invalid meas_value for meas_datatype")

          } else if (datatype == Datatype.String && encoded.meas_unit.nonEmpty) {
            JsError(__, "String measurement may not contain a meas_unit")

          } else if (datatype != Datatype.String && encoded.meas_unit.isEmpty) {
            JsError(__, "Measurement must contain a meas_unit")

          }
        }
        JsSuccess(encodedList)
    }
  }

  def toJson(sensorMeasurement: Model) = {
    import JsonFormats._
    Json.toJson(encodeSensorMeasurement(sensorMeasurement))
  }

  /** Convert a list of SensorMeasurement to a json representation. */
  def toJson(sensorMeasurements: List[Model]) = {
    import JsonFormats._
    Json.toJson(sensorMeasurements.map(encodeSensorMeasurement))
  }

  /** Convert a list of SensorMeasurementsSummary to a json representation. */
  def toMeasurementSummaryJson(sensorMeasurements: List[SensorMeasurementSummary]) = {
    import JsonFormats._
    Json.toJson(sensorMeasurements)
  }

  def toMeasurementCleansedJson(sensorMeasurements: List[SensorMeasurementCleansed]) = {
    import JsonFormats._
    Json.toJson(sensorMeasurements.map(encodeSensorMeasurementCleansed))
  }

  object JsonFormats {

    import _root_.util.JsonEitherFormat._

    case class EncodedSensorMeasurement(
      company: String,
      site: String,
      station: String,
      sensor: String,
      ts: Date,
      event: String,
      meas_name: String,
      meas_value: Either[BigDecimal, String],
      meas_datatype: String,
      meas_unit: Option[String],
      meas_status: Option[String],
      meas_lower_limit: Option[BigDecimal],
      meas_upper_limit: Option[BigDecimal],
      meas_description: Option[String]
    )

    case class EncodedSensorMeasurementCleansed(
      company: String,
      site: String,
      station: String,
      sensor: String,
      ts: Date,
      event: String,
      meas_name: String,
      meas_value: Either[BigDecimal, String],
      meas_datatype: String,
      meas_unit: Option[String],
      meas_status: Option[String],
      meas_flag: Option[String],
      meas_method: Option[String],
      meas_lower_limit: Option[BigDecimal],
      meas_upper_limit: Option[BigDecimal],
      meas_description: Option[String]
    )

    implicit def encodeSensorMeasurementCleansed(meas: SensorMeasurementCleansed): EncodedSensorMeasurementCleansed = {
      val sm = com.epidata.lib.models.SensorMeasurement.convertSensorMeasurementCleansedToSensorMeasurement(meas)
      val encodedSensorMeasurement = encodeSensorMeasurement(sm)
      EncodedSensorMeasurementCleansed(
        encodedSensorMeasurement.company,
        encodedSensorMeasurement.site,
        encodedSensorMeasurement.station,
        encodedSensorMeasurement.sensor,
        encodedSensorMeasurement.ts,
        encodedSensorMeasurement.event,
        encodedSensorMeasurement.meas_name,
        encodedSensorMeasurement.meas_value,
        encodedSensorMeasurement.meas_datatype,
        encodedSensorMeasurement.meas_unit,
        encodedSensorMeasurement.meas_status,
        meas.meas_flag,
        meas.meas_method,
        encodedSensorMeasurement.meas_lower_limit,
        encodedSensorMeasurement.meas_upper_limit,
        encodedSensorMeasurement.meas_description
      )
    }

    def convertNaNToDouble(double: Double): Double = {
      if (java.lang.Double.isNaN(double)) java.lang.Double.valueOf(0) else double
    }

    def convertAnyValToDouble(double: AnyVal): Double = {
      try {
        convertNaNToDouble(double.asInstanceOf[Double])
      } catch {
        case _: Throwable => 0
      }
    }

    implicit def encodeSensorMeasurement(meas: Model): EncodedSensorMeasurement =
      meas.meas_value match {

        case meas_value: Double =>
          EncodedSensorMeasurement(
            meas.company,
            meas.site,
            meas.station,
            meas.sensor,
            meas.ts,
            meas.event,
            meas.meas_name,
            Left(convertNaNToDouble(meas_value)), // return 0 for now.
            Datatype.Double.toString,
            meas.meas_unit,
            meas.meas_status,
            meas.meas_lower_limit.map(convertAnyValToDouble(_)),
            meas.meas_upper_limit.map(convertAnyValToDouble(_)),
            meas.meas_description
          )

        case meas_value: Long =>
          EncodedSensorMeasurement(
            meas.company,
            meas.site,
            meas.station,
            meas.sensor,
            meas.ts,
            meas.event,
            meas.meas_name,
            Left(meas_value),
            Datatype.Long.toString,
            meas.meas_unit,
            meas.meas_status,
            meas.meas_lower_limit.map(_.asInstanceOf[Long]),
            meas.meas_upper_limit.map(_.asInstanceOf[Long]),
            meas.meas_description
          )

        case meas_value: String =>
          EncodedSensorMeasurement(
            meas.company,
            meas.site,
            meas.station,
            meas.sensor,
            meas.ts,
            meas.event,
            meas.meas_name,
            Right(meas_value),
            Datatype.String.toString,
            None,
            meas.meas_status,
            None,
            None,
            meas.meas_description
          )

        case meas_value: Binary =>
          val (meas_datatype, meas_value_base64) = Binary.toBase64(meas_value)
          if (!Datatype.isBinary(meas_datatype)) {
            throw new IllegalArgumentException("Unexpected datatype.")
          }
          EncodedSensorMeasurement(
            meas.company,
            meas.site,
            meas.station,
            meas.sensor,
            meas.ts,
            meas.event,
            meas.meas_name,
            Right(meas_value_base64),
            meas_datatype.toString,
            meas.meas_unit,
            meas.meas_status,
            None,
            None,
            meas.meas_description
          )
      }

    implicit def encodeSensorMeasurementList(measList: List[Model]): List[EncodedSensorMeasurement] = {
      //      var encodedList = new ListBuffer[EncodedSensorMeasurement]
      measList.map { meas =>
        meas.meas_value match {

          case meas_value: Double =>
            EncodedSensorMeasurement(
              meas.company,
              meas.site,
              meas.station,
              meas.sensor,
              meas.ts,
              meas.event,
              meas.meas_name,
              Left(meas_value),
              Datatype.Double.toString,
              meas.meas_unit,
              meas.meas_status,
              meas.meas_lower_limit.map(_.asInstanceOf[Double]),
              meas.meas_upper_limit.map(_.asInstanceOf[Double]),
              meas.meas_description
            )
          //            encodedList.appendAll(EncodedSensorMeasurement)

          case meas_value: Long =>
            EncodedSensorMeasurement(
              meas.company,
              meas.site,
              meas.station,
              meas.sensor,
              meas.ts,
              meas.event,
              meas.meas_name,
              Left(meas_value),
              Datatype.Long.toString,
              meas.meas_unit,
              meas.meas_status,
              meas.meas_lower_limit.map(_.asInstanceOf[Long]),
              meas.meas_upper_limit.map(_.asInstanceOf[Long]),
              meas.meas_description
            )
          //           encodedList.appendAll(EncodedSensorMeasurement)

          case meas_value: String =>
            EncodedSensorMeasurement(
              meas.company,
              meas.site,
              meas.station,
              meas.sensor,
              meas.ts,
              meas.event,
              meas.meas_name,
              Right(meas_value),
              Datatype.String.toString,
              None,
              meas.meas_status,
              None,
              None,
              meas.meas_description
            )
          //            encodedList.appendAll(EncodedSensorMeasurement)

          case meas_value: Binary =>
            val (meas_datatype, meas_value_base64) = Binary.toBase64(meas_value)
            if (!Datatype.isBinary(meas_datatype)) {
              throw new IllegalArgumentException("Unexpected datatype.")
            }
            EncodedSensorMeasurement(
              meas.company,
              meas.site,
              meas.station,
              meas.sensor,
              meas.ts,
              meas.event,
              meas.meas_name,
              Right(meas_value_base64),
              meas_datatype.toString,
              meas.meas_unit,
              meas.meas_status,
              None,
              None,
              meas.meas_description
            )
          //            encodedList.appendAll(EncodedSensorMeasurement)
        }
      }
    }

    implicit def decodeSensorMeasurement(meas: EncodedSensorMeasurement): Model = {
      val datatype = Datatype.byName(meas.meas_datatype)
      val meas_value = datatype match {
        case Datatype.Double => meas.meas_value.left.get.toDouble
        case Datatype.Long => meas.meas_value.left.get.toLongExact
        case Datatype.String => meas.meas_value.right.get
        case _ => Binary.fromBase64(datatype, meas.meas_value.right.get)
      }
      val meas_lower_limit = datatype match {
        case Datatype.Double => meas.meas_lower_limit.map(_.toDouble)
        case Datatype.Long => meas.meas_lower_limit.map(_.toLongExact)
        case _ =>
          if (meas.meas_lower_limit.isDefined) Logger.warn(
            "parseJson: Unexpected meas_lower_limit on non numeric measurement."
          )
          None
      }
      val meas_upper_limit = datatype match {
        case Datatype.Double => meas.meas_upper_limit.map(_.toDouble)
        case Datatype.Long => meas.meas_upper_limit.map(_.toLongExact)
        case _ =>
          if (meas.meas_upper_limit.isDefined) Logger.warn(
            "parseJson: Unexpected meas_upper_limit on non numeric measurement."
          )
          None
      }
      Model(
        meas.company,
        meas.site,
        meas.station,
        meas.sensor,
        meas.ts,
        meas.event,
        meas.meas_name,
        meas_value,
        meas.meas_unit,
        meas.meas_status,
        meas_lower_limit,
        meas_upper_limit,
        meas.meas_description
      )
    }

    implicit def decodeSensorMeasurementList(measList: List[EncodedSensorMeasurement]): List[Model] = {
      measList.map { meas =>
        val datatype = Datatype.byName(meas.meas_datatype)
        val meas_value = datatype match {
          case Datatype.Double => meas.meas_value.left.get.toDouble
          case Datatype.Long => meas.meas_value.left.get.toLongExact
          case Datatype.String => meas.meas_value.right.get
          case _ => Binary.fromBase64(datatype, meas.meas_value.right.get)
        }
        val meas_lower_limit = datatype match {
          case Datatype.Double => meas.meas_lower_limit.map(_.toDouble)
          case Datatype.Long => meas.meas_lower_limit.map(_.toLongExact)
          case _ =>
            if (meas.meas_lower_limit.isDefined) Logger.warn(
              "parseJson: Unexpected meas_lower_limit on non numeric measurement."
            )
            None
        }
        val meas_upper_limit = datatype match {
          case Datatype.Double => meas.meas_upper_limit.map(_.toDouble)
          case Datatype.Long => meas.meas_upper_limit.map(_.toLongExact)
          case _ =>
            if (meas.meas_upper_limit.isDefined) Logger.warn(
              "parseJson: Unexpected meas_upper_limit on non numeric measurement."
            )
            None
        }
        Model(
          meas.company,
          meas.site,
          meas.station,
          meas.sensor,
          meas.ts,
          meas.event,
          meas.meas_name,
          meas_value,
          meas.meas_unit,
          meas.meas_status,
          meas_lower_limit,
          meas_upper_limit,
          meas.meas_description
        )
      }
    }

    def timestampToDate(t: java.sql.Timestamp): Date = new Date(t.getTime)
    def dateToTimestamp(dt: Date): java.sql.Timestamp = new java.sql.Timestamp(dt.getTime)

    implicit val sensorMeasurementFormat: Format[EncodedSensorMeasurement] = Json.format[EncodedSensorMeasurement]
    implicit val sensorMeasurementCleansedFormat: Format[EncodedSensorMeasurementCleansed] = Json.format[EncodedSensorMeasurementCleansed]
    implicit val timestampFormat = new Format[java.sql.Timestamp] {
      def writes(t: java.sql.Timestamp): JsValue = Json.toJson(timestampToDate(t))
      def reads(json: JsValue): JsResult[java.sql.Timestamp] = Json.fromJson[Date](json).map(dateToTimestamp)
    }
    implicit val sensorMeasurementsSummaryFormat: Format[SensorMeasurementSummary] = Json.format[SensorMeasurementSummary]
  }

}
