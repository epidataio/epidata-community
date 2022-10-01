/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package util

import java.util.concurrent.{ CancellationException, Future => JFuture, TimeUnit }
import io.netty.util.{ Timeout, TimerTask, HashedWheelTimer }
import scala.concurrent.{ Promise, Future }
import scala.util.Try

object Implicits {

  private val pollIntervalMs = 100L
  private val timer = new HashedWheelTimer(pollIntervalMs, TimeUnit.MILLISECONDS)

  implicit class JFutureHelpers[T](javaFuture: JFuture[T]) {
    def toScala: Future[T] = {
      val promise = Promise[T]()

      def checkCompletion(): Unit = {
        if (javaFuture.isCancelled) {
          promise.failure(new CancellationException())
        } else if (javaFuture.isDone) {
          promise.complete(Try(javaFuture.get))
        } else {
          scheduleTimeout()
        }
        ()
      }

      def scheduleTimeout(): Unit = {
        timer.newTimeout(new TimerTask {
          override def run(timeout: Timeout): Unit = checkCompletion()
        }, pollIntervalMs, TimeUnit.MILLISECONDS)
        ()
      }

      checkCompletion()
      promise.future
    }
  }

}
