package controllers

import javax.inject._

import play.api.data.Form
import play.api.mvc.{ AnyContent, AbstractController, RequestHeader, Request }
import play.twirl.api.{ Html, Txt }
import securesocial.controllers._
import securesocial.core._
import play.api.i18n.{ I18nSupport, Messages, Lang, MessagesApi }
import play.api.{ Configuration, Environment }

@Singleton
class Users @Inject() (env: RuntimeEnvironment)(implicit val conf: Configuration, lang: Lang, assets: AssetsFinder, messagesAPI: MessagesApi) extends ViewTemplates.Default(env) {
  /**
   * Returns the html for the login page
   * @param request
   * @tparam A
   * @return
   */
  override def getLoginPage(
    form: Form[(String, String)],
    msg: Option[String] = None)(implicit request: RequestHeader): Html = {
    views.html.User.login(form, msg)
  }

  /**
   * Returns the html for the signup page
   *
   * @param request
   * @tparam A
   * @return
   */
  override def getSignUpPage(
    form: Form[RegistrationInfo],
    token: String)(implicit request: RequestHeader): Html = {
    throw new NotImplementedError("getSignUpPage")
  }

  /**
   * Returns the html for the start signup page
   *
   * @param request
   * @tparam A
   * @return
   */
  override def getStartSignUpPage(form: Form[String])(implicit request: RequestHeader): Html = {
    throw new NotImplementedError("getStartSignUpPage")
  }

  /**
   * Returns the html for the reset password page
   *
   * @param request
   * @tparam A
   * @return
   */
  override def getStartResetPasswordPage(form: Form[String])(implicit request: RequestHeader): Html = {
    throw new NotImplementedError("getStartResetPasswordPage")
  }

  /**
   * Returns the html for the start reset page
   *
   * @param request
   * @tparam A
   * @return
   */
  override def getResetPasswordPage(form: Form[(String, String)], token: String)(implicit request: RequestHeader): Html = {
    throw new NotImplementedError("getResetPasswordPage")
  }

  /**
   * Returns the html for the change password page
   *
   * @param request
   * @param form
   * @tparam A
   * @return
   */
  override def getPasswordChangePage(form: Form[ChangeInfo])(implicit request: RequestHeader): Html = {
    throw new NotImplementedError("getPasswordChangePage")
  }

  /**
   * Returns the html of the Not Authorized page
   *
   * @param request the current http request
   * @return a String with the text and/or html body for the email
   */
  override def getNotAuthorizedPage(implicit request: RequestHeader): Html = {
    throw new NotImplementedError("getNotAuthorizedPage")
  }

}
