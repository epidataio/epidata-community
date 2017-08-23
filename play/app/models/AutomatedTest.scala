/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import com.epidata.lib.models.{ AutomatedTest => Model }
import com.epidata.lib.models.util.Binary
import com.epidata.lib.models.util.Datatype
import java.util.Date
import play.api.Logger
import play.api.libs.functional.syntax._
import play.api.libs.json._
import scala.util.matching.Regex
import _root_.util.Ordering

object AutomatedTest {

  import com.epidata.lib.models.AutomatedTest._

  /**
   * Insert an automated test measurement into the database.
   * @param automatedTest The AutomatedTest to insert.
   */
  def insert(automatedTest: Model): Unit = Measurement.insert(automatedTest)

  /**
   * Find automated tests in the database matching the specified parameters.
   * @param company
   * @param site
   * @param device_group
   * @param tester
   * @param beginTime Beginning of query time interval, inclusive
   * @param endTime End of query time interval, exclusive
   * @param ordering Timestamp ordering of results, if specified.
   */
  def find(
    company: String,
    site: String,
    device_group: String,
    tester: String,
    beginTime: Date,
    endTime: Date,
    ordering: Ordering.Value
  ): List[Model] =
    Measurement.find(company, site, device_group, tester, beginTime, endTime, ordering)
      .map(measurementToAutomatedTest)

  /** Parse the json representation of an automated test to an AutomatedTest. */
  def parseJson(json: JsValue): JsResult[Model] = {
    import JsonFormats._
    json.validate[EncodedAutomatedTest] match {
      case error: JsError => error

      case JsSuccess(encoded: EncodedAutomatedTest, _) if !Datatype.isValidName(encoded.meas_datatype) =>
        JsError(__, s"Could not parse datatype ${encoded.meas_datatype}")

      case JsSuccess(encoded: EncodedAutomatedTest, _) =>
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

  /** Convert a list of AutomatedTest to a json representation. */
  def toJson(automatedTests: List[Model]) = {
    import JsonFormats._
    Json.toJson(automatedTests.map(encodeAutomatedTest))
  }

  private object JsonFormats {

    import play.api.libs.json.Reads._
    import _root_.util.JsonEitherFormat._

    case class EncodedAutomatedTest(
      company: String,
      site: String,
      device_group: String,
      tester: String,
      ts: Date,
      device_name: String,
      test_name: String,
      meas_name: String,
      meas_value: Either[BigDecimal, String],
      meas_datatype: String,
      meas_unit: Option[String],
      meas_status: Option[String],
      meas_lower_limit: Option[BigDecimal],
      meas_upper_limit: Option[BigDecimal],
      meas_description: Option[String],
      device_status: Option[String],
      test_status: Option[String]
    )

    implicit def encodeAutomatedTest(test: Model): EncodedAutomatedTest =
      test.meas_value match {

        case meas_value: Double =>
          EncodedAutomatedTest(
            test.company,
            test.site,
            test.device_group,
            test.tester,
            test.ts,
            test.device_name,
            test.test_name,
            test.meas_name,
            Left(meas_value),
            Datatype.Double.toString,
            test.meas_unit,
            test.meas_status,
            test.meas_lower_limit.map(_.asInstanceOf[Double]),
            test.meas_upper_limit.map(_.asInstanceOf[Double]),
            test.meas_description,
            test.device_status,
            test.test_status
          )

        case meas_value: Long =>
          EncodedAutomatedTest(
            test.company,
            test.site,
            test.device_group,
            test.tester,
            test.ts,
            test.device_name,
            test.test_name,
            test.meas_name,
            Left(meas_value),
            Datatype.Long.toString,
            test.meas_unit,
            test.meas_status,
            test.meas_lower_limit.map(_.asInstanceOf[Long]),
            test.meas_upper_limit.map(_.asInstanceOf[Long]),
            test.meas_description,
            test.device_status,
            test.test_status
          )

        case meas_value: String =>
          EncodedAutomatedTest(
            test.company,
            test.site,
            test.device_group,
            test.tester,
            test.ts,
            test.device_name,
            test.test_name,
            test.meas_name,
            Right(meas_value),
            Datatype.String.toString,
            None,
            test.meas_status,
            None,
            None,
            test.meas_description,
            test.device_status,
            test.test_status
          )

        case meas_value: Binary =>
          val (meas_datatype, meas_value_base64) = Binary.toBase64(meas_value)
          if (!Datatype.isBinary(meas_datatype)) {
            throw new IllegalArgumentException("Unexpected datatype.")
          }
          EncodedAutomatedTest(
            test.company,
            test.site,
            test.device_group,
            test.tester,
            test.ts,
            test.device_name,
            test.test_name,
            test.meas_name,
            Right(meas_value_base64),
            meas_datatype.toString,
            test.meas_unit,
            test.meas_status,
            None,
            None,
            test.meas_description,
            test.device_status,
            test.test_status
          )
      }

    implicit def decodeAutomatedTest(test: EncodedAutomatedTest): Model = {
      val datatype = Datatype.byName(test.meas_datatype)
      val meas_value = datatype match {
        case Datatype.Double => test.meas_value.left.get.toDouble
        case Datatype.Long => test.meas_value.left.get.toLongExact
        case Datatype.String => test.meas_value.right.get
        case _ => Binary.fromBase64(datatype, test.meas_value.right.get)
      }
      val meas_lower_limit = datatype match {
        case Datatype.Double => test.meas_lower_limit.map(_.toDouble)
        case Datatype.Long => test.meas_lower_limit.map(_.toLongExact)
        case _ =>
          if (test.meas_lower_limit.isDefined) Logger.warn(
            "parseJson: Unexpected meas_lower_limit on non numeric measurement."
          )
          None
      }
      val meas_upper_limit = datatype match {
        case Datatype.Double => test.meas_upper_limit.map(_.toDouble)
        case Datatype.Long => test.meas_upper_limit.map(_.toLongExact)
        case _ =>
          if (test.meas_upper_limit.isDefined) Logger.warn(
            "parseJson: Unexpected meas_upper_limit on non numeric measurement."
          )
          None
      }
      Model(
        test.company,
        test.site,
        test.device_group,
        test.tester,
        test.ts,
        test.device_name,
        test.test_name,
        test.meas_name,
        Some(test.meas_datatype),
        meas_value,
        test.meas_unit,
        test.meas_status,
        meas_lower_limit,
        meas_upper_limit,
        test.meas_description,
        test.device_status,
        test.test_status
      )
    }

    implicit val automatedTestFormat: Format[EncodedAutomatedTest] = Format(
      automatedTestReads, automatedTestWrites
    )

    private val statusRegex = "^(PASS|FAIL)$".r

    private lazy val automatedTestReads: Reads[EncodedAutomatedTest] = (
      (__ \ 'company).read[String] and
      (__ \ 'site).read[String] and
      (__ \ 'device_group).read[String] and
      (__ \ 'tester).read[String] and
      (__ \ 'ts).read[Date] and
      (__ \ 'device_name).read[String] and
      (__ \ 'test_name).read[String] and
      (__ \ 'meas_name).read[String] and
      (__ \ 'meas_value).read[Either[BigDecimal, String]] and
      (__ \ 'meas_datatype).read[String] and
      (__ \ 'meas_unit).readNullable[String] and
      (__ \ 'meas_status).readNullable[String](pattern(statusRegex)) and
      (__ \ 'meas_lower_limit).readNullable[BigDecimal] and
      (__ \ 'meas_upper_limit).readNullable[BigDecimal] and
      (__ \ 'meas_description).readNullable[String] and
      (__ \ 'device_status).readNullable[String](pattern(statusRegex)) and
      (__ \ 'test_status).readNullable[String](pattern(statusRegex))
    )(EncodedAutomatedTest.apply _)

    private lazy val automatedTestWrites: Writes[EncodedAutomatedTest] = (
      (__ \ 'company).write[String] and
      (__ \ 'site).write[String] and
      (__ \ 'device_group).write[String] and
      (__ \ 'tester).write[String] and
      (__ \ 'ts).write[Date] and
      (__ \ 'device_name).write[String] and
      (__ \ 'test_name).write[String] and
      (__ \ 'meas_name).write[String] and
      (__ \ 'meas_value).write[Either[BigDecimal, String]] and
      (__ \ 'meas_datatype).write[String] and
      (__ \ 'meas_unit).writeNullable[String] and
      (__ \ 'meas_status).writeNullable[String] and
      (__ \ 'meas_lower_limit).writeNullable[BigDecimal] and
      (__ \ 'meas_upper_limit).writeNullable[BigDecimal] and
      (__ \ 'meas_description).writeNullable[String] and
      (__ \ 'device_status).writeNullable[String] and
      (__ \ 'test_status).writeNullable[String]
    )(unlift(EncodedAutomatedTest.unapply))
  }

}
