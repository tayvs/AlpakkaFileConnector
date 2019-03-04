package test.alpakka.file.connectors

import java.io.File
import java.nio.file.{FileSystem, FileSystems, Path}

import akka.NotUsed
import akka.actor.{Actor, ActorLogging}
import akka.pattern._
import akka.persistence.{PersistentActor, RecoveryCompleted}
import akka.stream.{ActorMaterializer, KillSwitches, UniqueKillSwitch}
import akka.stream.alpakka.file.scaladsl.FileTailSource
import akka.stream.scaladsl.{FileIO, Flow, Framing, Keep, Sink}
import akka.util.{ByteString, Timeout}
import test.alpakka.file.connectors.Parser.Commands.{ChangeFile, Lines}
import test.alpakka.file.connectors.Parser.Events.{FileChanged, IncOffset}
import test.alpakka.file.connectors.Parser.State.CurrentFile
import test.alpakka.file.connectors.Parser._
import test.alpakka.file.connectors.Parser.StreamControl._

import scala.concurrent.duration._
import scala.collection.breakOut
import scala.util.{Failure, Success}

class Parser extends PersistentActor with ActorLogging {

  import context.dispatcher

  val fs: FileSystem = FileSystems.getDefault
  val fileName = "resources/file"

  override def persistenceId: String = "parser"

  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val timeout: Timeout = Timeout(2 seconds)

  var currentFile = CurrentFile(FileCursor(fs.getPath(fileName), 0l), initCurrentReader(fs.getPath(fileName), 0l))

  def updateState(ev: PersistedEvents): Unit = currentFile = ev match {
    case cf: State.CurrentFile        => cf.copy(control = initCurrentReader(cf.cursor))
    case IncOffset(path, offsetDelta) => currentFile.copy(cursor = currentFile.cursor.copy(offset = currentFile.cursor.offset + offsetDelta))
    case FileChanged(newPath)         => currentFile.copy(cursor = FileCursor(newPath))
  }

  override def receiveRecover: Receive = {
    case ev: PersistedEvents => updateState(ev)
  }

  def streamControlReceive: Receive = {
    case Init     => sender() ! Ack
    case Complete => log.warning(s"Current file ${currentFile.cursor.path} listening ended")
  }

  override def receiveCommand: Receive = {
    case str: String         =>
      persist(IncOffset(currentFile.cursor.path, str.length + 1)) { ev =>
        updateState(ev)
        sender ! Ok
        println(s"str $str. $currentFile")
      }
    case Lines(path, lines)  =>
      lines.foreach { str =>
        persist(IncOffset(path, str.length + 1)) { ev =>
          updateState(ev)
          sender() ! Ok
          context.system.eventStream.publish(str)
        }
      }
      defer(()) { _ => sender() ! Ack }
    case ChangeFile(newPath) =>
      persist(FileChanged(newPath)) { ev =>
        updateState(ev)
        sender ! Ack
      }
  }

  def initCurrentReader(fileCursor: FileCursor): UniqueKillSwitch = initCurrentReader(fileCursor.path, fileCursor.offset)

  def initCurrentReader(path: Path, offset: Long = 0l): UniqueKillSwitch =
    FileTailSource(
      path = path,
      maxChunkSize = 8192,
      startingPosition = offset,
      pollingInterval = 250.millis
    )
      .toMat(processingSink(path))(Keep.right)
      .run()

  def initHistoryReader(fileCursor: FileCursor): UniqueKillSwitch = initHistoryReader(fileCursor.path, fileCursor.offset)

  def initHistoryReader(path: Path, offset: Long = 0l): UniqueKillSwitch =
    FileIO
      .fromPath(path, 8192, offset)
      .toMat(processingSink(path))(Keep.right)
      .run()

  def processingSink(path: Path): Sink[ByteString, UniqueKillSwitch] = Flow[ByteString]
    .via(Framing.delimiter(ByteString("\n"), Int.MaxValue))
    .map(_.utf8String)
    .groupedWithin(100, 250.millis)
    .map(Lines(path, _))
    .viaMat(KillSwitches.single)(Keep.right)
    .toMat(Sink.actorRefWithAck(self, Init, Ack, Complete))(Keep.left)
}

object Parser {

  sealed trait StreamControl

  object StreamControl {

    case object Init extends StreamControl

    case object Ack extends StreamControl

    case object Complete extends StreamControl

  }

  case object Ok

  sealed trait PersistedEvents

  sealed trait Events extends PersistedEvents

  object Events {

    case class IncOffset(path: Path, offsetDelta: Int) extends Events

    case class FileChanged(newPath: Path) extends Events

  }

  sealed trait State extends PersistedEvents

  case class FileCursor(path: Path, offset: Long = 0l)

  object State {

    case object EmptyState extends State

    case class CurrentFile(cursor: FileCursor, control: UniqueKillSwitch) extends State

    case class CurrentWithHistory(history: Map[Path, FileCursor]) extends State

  }

  sealed trait Commands

  object Commands {

    case class Lines(path: Path, lines: Seq[String]) extends Commands

    case class ChangeFile(nrePath: Path) extends Commands

  }

}
