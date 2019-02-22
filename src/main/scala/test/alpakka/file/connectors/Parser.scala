package test.alpakka.file.connectors

import java.nio.file.FileSystems

import akka.NotUsed
import akka.actor.Actor
import akka.persistence.{PersistentActor, RecoveryCompleted}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.file.scaladsl.FileTailSource
import akka.stream.scaladsl.{FileIO, Framing}
import akka.util.ByteString

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Parser extends Actor {

  import context.dispatcher

  implicit val mat = ActorMaterializer()
  val fileName = "resources/file"

  //  override def persistenceId: String = "parser"
  //
  //  override def receiveRecover: Receive = {
  //    case RecoveryCompleted =>
  //      val fs = FileSystems.getDefault
  //      FileTailSource(
  //        path = fs.getPath(fileName),
  //        maxChunkSize = 8192,
  //        startingPosition = 0,
  //        pollingInterval = 250.millis
  //      )
  //        .via(Framing.delimiter(ByteString("\n"), Int.MaxValue))
  //        .map(_.utf8String)
  //        .runForeach(self ! _)
  //        .foreach(_ => println("stream ended"))
  //  }
  //
  //  override def receiveCommand: Receive = {
  //    case str: String => println(str)
  //  }

  val fs = FileSystems.getDefault
    FileTailSource(
      path = fs.getPath(fileName),
      maxChunkSize = 20,
      startingPosition = 0,
      pollingInterval = 250.millis
    )
//  FileIO.fromPath(fs.getPath(fileName), 8192, 0)
    .via(Framing.delimiter(ByteString("\n"), 100, true))
    .map(_.utf8String)
    .runForeach(self ! _)
    .andThen {
      case Success(value) => println(s"stream ended. $value")
      case Failure(ex)    => ex.printStackTrace()
    }
  //    .foreach(_ => println("stream ended"))

  override def receive: Receive = {
    case str: String => println(str)
  }
}
