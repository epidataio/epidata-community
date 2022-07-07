import play.api.mvc._
class DeviceRequest[A](val jwt_token: String, request: Request[A]) extends WrappedRequest[A](request)
class UserAction @Inject()(val parser: BodyParsers.Default)(implicit val executionContext: ExecutionContext)
    extends ActionBuilder[DeviceRequest, AnyContent]
    with ActionTransformer[Request, DeviceRequest] {
    def transform[A](request: Request[A]) = Future.successful {
        new DeviceRequest(request.session.get(“jwt_token”), request)
    }
}
class ValidAction (implicit val conf: Configuration) {
    def validate(implicit ec: ExecutionContext)  = new ActionFilter[DeviceRequest] {
        def executionContext = ec
        def filter[A](input: DeviceRequest[A]) = Future.successful {
            //app.conf JwtSecretKey
            val JwtSecretKey = conf.get[String](“application.secret”)
            if (!JsonWebToken.validate(input.jwt_token, JwtSecretKey))
                Some(Forbidden)
            else
                DeviceService.updateDevice(DeviceService.queryToken(input.jwt_token), input.jwt_token, 0/*time*/)
            }
    }
}
