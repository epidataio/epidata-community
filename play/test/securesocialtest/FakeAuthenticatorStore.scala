// Based on https://github.com/jaliss/securesocial/commit/77ce2f94d6a0e1f0fb032dbdec344c953dea7771

/****
package securesocialtest

import play.api.Application
import securesocial.core._

class FakeAuthenticatorStore(app: Application) extends AuthenticatorStore(app) {
  var authenticator: Option[Authenticator] = None
  def save(authenticator: Authenticator): Either[Error, Unit] = {
    this.authenticator = Some(authenticator)
    Right()
  }
  def find(id: String): Either[Error, Option[Authenticator]] = {
    Some(authenticator.filter(_.id == id)).toRight(new Error("no such authenticator"))
  }
  def delete(id: String): Either[Error, Unit] = {
    this.authenticator = None
    Right()
  }
}
****/
