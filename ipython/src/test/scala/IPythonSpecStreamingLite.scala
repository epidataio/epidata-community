package ipython

import com.epidata.spark.{ EpidataLiteContext, Measurement }
//import java.nio.ByteBuffer
import java.sql.Timestamp

import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import matchers.should._

import scala.sys.process.Process

import scala.io.Source

object IPythonSpecStreamingLite extends App {

  try {

    println("Hello")
    //    Process(List("python",
    //      "ipython/test/test_streaming_lite.py")
    //    ).run()
    Process(List(
      "python",
      "ipython/test/test_s.py")).run()

  } catch {
    case e: Exception =>

      System.err.println(e.getMessage)

  }

}
