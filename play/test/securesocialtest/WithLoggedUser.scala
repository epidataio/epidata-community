// Based on https://github.com/jaliss/securesocial/commit/77ce2f94d6a0e1f0fb032dbdec344c953dea7771

/****
package securesocialtest

import org.specs2.execute.{ AsResult, Result }
import org.specs2.mock.Mockito
import play.api.test._
import securesocial.core._

abstract class WithLoggedUser(override val app: FakeApplication = FakeApplication(), val identity: Option[Identity] = None) extends WithApplication(app) with Mockito {

  lazy val user = identity getOrElse SocialUserGenerator.socialUser()
  lazy val mockUserService = mock[UserService]

  def cookie = {
    println("cookie"); Authenticator.create(user) match {
      case Right(authenticator) => authenticator.toCookie
      case _ => throw new IllegalArgumentException("Your FakeApplication _must_ configure a working AuthenticatorStore")
    }
  }

  override def around[T: AsResult](t: => T): org.specs2.execute.Result = super.around {
    mockUserService.find(user.identityId) returns Some(user)
    println("setting service")
    UserService.setService(mockUserService)
    t
  }
}

object WithLoggedUser {
  val excludedPlugins = List("securesocial.core.DefaultAuthenticatorStore")
  val includedPlugins = List("securesocialtest.FakeAuthenticatorStore")
  def minimalApp = new FakeApplication(withoutPlugins = excludedPlugins, additionalPlugins = includedPlugins)
}
****/
