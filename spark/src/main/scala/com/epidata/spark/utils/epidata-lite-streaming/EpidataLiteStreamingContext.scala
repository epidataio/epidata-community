import java.util
import java.util.concurrent.Executors

import org.zeromq.ZMQ
import com.epidata.spark.ops.Transformation
import org.apache.cassandra.db.transform.Transformation

import scala.collection.BitSet.empty
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`

case class Message(topic: Object, key: Object, value: Object)

class EpidataLiteStreamingContext {
  var port: Integer = 5550
  val receiver: StreamingNode.type = _
  val processors: util.ArrayList[StreamingNode.type] = _
  var runStream: Boolean = _
  val context: ZMQ.context = _

  def init(): Unit = { //ec.start_streaming()
    context = ZMQ.context(1)
    runStream = true
  }

  def createTransformations(opName: String, meas_names: List[String], params: Map[String, String]): Transformation {
    // create and return a transformation object
    opName match {
    case "Identity" => new Identity()
    case "FillMissingValue" => new FillMissingValue(meas_names, "rolling", 3)
    case "OutlierDetector" => new OutlierDetector("meas_value", "quartile")
    case "MeasStatistics" => new MeasStatistics(meas_names, "standard")
  }

//  def createCustomTransformation(opName: String, transformation: Transformation): Transformation {
//    transformation
//  }

  }

//  def createStream(sourceTopic: String, destinationTopic: String, operations: Array[Transformation]): Unit {
//    processors.add(new StreamingNode(context, port, port, ))
//  }

  def createStream(sourceTopic: String, destinationTopic: String, operation: Transformation): Unit = {
    if (processors.size == 0) {
      processors.add(new StreamingNode(context, port.toString, (port + 2).toString, sourceTopic, destinationTopic, Transformation))
    }
    else if (destinationTopic.equals("measurements_substituted") || destinationTopic.equals("measurement_cleansed") || destinationTopic.equals("measurements_summary")) {
      processors.add(new StreamingNode(context, port.toString, (5551).toString, sourceTopic, destinationTopic, Transformation))
    }
    else {
      processors.add(new StreamingNode(context, port.toString, (port + 1).toString, sourceTopic, destinationTopic, Transformation))
    }
    port += 1
  }

  def startStream(): Unit = {
    var i = 0
    processors.reverse
    //iterate through processors arraylist backwards creating thread
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (runStream) {
          for (processor <- processors) {
            processor.receive()
          }
        }
      }
    })
  }

  def stopStream(): Unit = {
    runStream = false //dont think you can stop thread internally in runtime, it needs to be prescripted
                      // process needs to be inturrupted externally
  }



}
