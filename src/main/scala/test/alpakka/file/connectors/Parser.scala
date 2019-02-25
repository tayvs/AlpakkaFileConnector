package test.alpakka.file.connectors

import java.io.File
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

  implicit val mat: ActorMaterializer = ActorMaterializer()
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

  var offset = 0l

  val fs = FileSystems.getDefault
  FileTailSource(
    path = fs.getPath(fileName),
    maxChunkSize = 0,
    startingPosition = offset,
    pollingInterval = 250.millis
  )
    .via(Framing.delimiter(ByteString("\n"), 100, true))
    .map(_.utf8String)
    .runForeach(self ! _)
    .andThen {
      case Success(value) => println(s"stream ended. $value")
      case Failure(ex)    => ex.printStackTrace()
    }

  override def receive: Receive = {
    case str: String =>
      offset += str.length + 1
      println(s"str $str offset $offset")
  }
}
