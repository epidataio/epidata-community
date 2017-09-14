package controllers

import _root_.util.EpidataMetrics
import play.api.mvc.{ Action, Controller }
import play.api.libs.json._

object MetricController extends Controller with securesocial.core.SecureSocial {
  def getMetric() = Action {
    Ok(Json.toJson(EpidataMetrics.getMetric))
  }
}
