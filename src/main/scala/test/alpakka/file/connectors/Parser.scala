package test.alpakka.file.connectors

import java.io.File
import java.nio.file.FileSystems

import akka.NotUsed
import akka.actor.Actor
import akka.pattern._
import akka.persistence.{PersistentActor, RecoveryCompleted}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.file.scaladsl.FileTailSource
import akka.stream.scaladsl.{Framing, Keep, Sink}
import akka.util.{ByteString, Timeout}
import test.alpakka.file.connectors.Parser.{IncOffset, Ok, PersistedEvents}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Parser extends PersistentActor {
  import context.dispatcher
  override def persistenceId: String = "parser"

  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val timeout: Timeout = Timeout(2 seconds)
  val fileName = "resources/file"
  var offset = 0l

  def updateState(ev: PersistedEvents): Unit = ev match {
    case snapshot: Parser.Offset      => offset = snapshot.offset
    case incOffset: Parser.IncOffset  => offset += incOffset.offsetDelta
  }

  override def receiveRecover: Receive = {
    case RecoveryCompleted =>
      val fs = FileSystems.getDefault
      FileTailSource(
        path = fs.getPath(fileName),
        maxChunkSize = 8192,
        startingPosition = 0,
        pollingInterval = 250.millis
      )
        .via(Framing.delimiter(ByteString("\n"), Int.MaxValue))
        .map(_.utf8String)
        .mapAsync(1) { line => (self ? line).mapTo[Ok.type] }
        .toMat(Sink.ignore)(Keep.right)
        .run()
        .andThen {
          case Success(value) => println(s"stream ended. $value")
          case Failure(ex) => ex.printStackTrace()
        }
    case ev: PersistedEvents => updateState(ev)
  }

  override def receiveCommand: Receive = {
    case str: String =>
      persist(IncOffset(str.length + 1)) { ev =>
        updateState(ev)
        sender ! Ok
        println(s"str $str offset $offset")
      }
  }
}

object Parser {

  case object Ok

  sealed trait PersistedEvents
  sealed trait Snapshot extends PersistedEvents
  sealed trait Events extends PersistedEvents

  case class IncOffset(offsetDelta: Int) extends Events
  case class Offset(offset: Long) extends Snapshot

}
