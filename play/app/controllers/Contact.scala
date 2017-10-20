/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import play.api.data._
import play.api.data.Forms._
import play.api.libs.mailer._
import play.api.mvc._

case class Contact(
  email: String,
  message: String
)

/** Controller to display the Notebook. */
object Contact extends Controller with securesocial.core.SecureSocial {

  val contactForm: Form[Contact] = Form(
    mapping(
      "Email" -> email,
      "Message" -> nonEmptyText
    )(Contact.apply)(Contact.unapply)
  )

  def show = SecuredAction { implicit request =>
    Ok(views.html.Contact.show(contactForm))
  }

  def submit = SecuredAction { implicit request =>
    import play.api.Play.current
    contactForm.bindFromRequest.fold(
      formWithErrors => {
        BadRequest(views.html.Contact.show(formWithErrors))
      },
      contact => {
        val email = Email(
          "epidata.co contact message",
          contact.email,
          Seq("info@epidata.co"),
          bodyText = Some(contact.message)
        )
        MailerPlugin.send(email)
        Redirect(routes.Contact.show).flashing("success" -> "Message Sent")
      }
    )
  }

}
