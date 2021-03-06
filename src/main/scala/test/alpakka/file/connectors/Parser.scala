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
import test.alpakka.file.connectors.Parser.Events.{FileChanged, FileReaded, IncOffset}
import test.alpakka.file.connectors.Parser.State.{CurrentFile, CurrentWithHistory}
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

  var state: State = {
    val path = fs.getPath(fileName)
    CurrentFile(FileCursor(path), initCurrentReader(path))
  }

  /** */
  def updateState(ev: Events): Unit =
    state = (state match {
      case State.EmptyState         => updateEmptyState()
      case cf: CurrentFile          => updateActiveState(cf)
      case cfwh: CurrentWithHistory => updateActiveWithHistoryState(cfwh)
    }) (ev)

  def updateActiveState(currentFile: CurrentFile): PartialFunction[Events, State] = {
    case IncOffset(path, offsetDelta) if currentFile.cursor.path == path =>
      val cursor = currentFile.cursor
      currentFile.copy(cursor = cursor.copy(offset = cursor.offset + offsetDelta))
    case FileChanged(newPath)                                            =>
      CurrentFile(cursor = FileCursor(newPath), control = initCurrentReader(FileCursor(newPath)))
  }

  def updateEmptyState(): PartialFunction[Events, State] = {
    case FileChanged(newPath) => CurrentFile(cursor = FileCursor(newPath), control = initCurrentReader(newPath))
  }

  def updateActiveWithHistoryState(activeWithHistory: CurrentWithHistory): PartialFunction[Events, State] = {
    case FileChanged(newPath)                                                          =>
      activeWithHistory.copy(current = CurrentFile(FileCursor(newPath), initCurrentReader(newPath)))
    case IncOffset(path, offsetDelta) if activeWithHistory.current.cursor.path == path =>
      val cursor = activeWithHistory.current.cursor
      activeWithHistory.copy(current = activeWithHistory.current.copy(cursor = cursor.copy(offset = cursor.offset + offsetDelta)))
    case IncOffset(path, offsetDelta) if activeWithHistory.history.contains(path)      =>
      val recordForUpdating = activeWithHistory.history(path)
      val updatedRecord = recordForUpdating.copy(offset = recordForUpdating.offset + offsetDelta)
      activeWithHistory.copy(history = activeWithHistory.history.updated(path, updatedRecord))
    case FileReaded(path)                                                              =>
      activeWithHistory.copy(history = activeWithHistory.history - path)
  }

  override def receiveRecover: Receive = {
    case State.EmptyState                                     => State.EmptyState
    case CurrentFile(cursor, _)                               => CurrentFile(cursor, initCurrentReader(cursor))
    case cwh@CurrentWithHistory(_, cf@CurrentFile(cursor, _)) => cwh.copy(current = cf.copy(control = initCurrentReader(cursor)))
    case ev: Events                                           => updateState(ev)
  }

  def streamControlReceive: Receive = {
    case Init               => sender() ! Ack
    case TailingCompleted   => log.warning(s"Current file $state listening ended")
    case fr: FileReaded     =>
      persist(fr) { ev =>
        updateState(ev)
        sender ! Ack
      }
    case Lines(path, lines) =>
      lines.foreach { str =>
        persist(IncOffset(path, str.length + 1)) { ev =>
          updateState(ev)
          sender() ! Ok
          context.system.eventStream.publish(str)
        }
      }
      defer(()) { _ => sender() ! Ack }
  }

  override def receiveCommand: Receive = {
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
      .viaMat(processing(path))(Keep.right)
      .toMat(Sink.actorRefWithAck(self, Init, Ack, TailingCompleted))(Keep.left)
      .run()

  def initHistoryReader(fileCursor: FileCursor): UniqueKillSwitch = initHistoryReader(fileCursor.path, fileCursor.offset)

  def initHistoryReader(path: Path, offset: Long = 0l): UniqueKillSwitch =
    FileIO
      .fromPath(path, 8192, offset)
      .viaMat(processing(path))(Keep.right)
      .toMat(Sink.actorRefWithAck(self, Init, Ack, FileReaded(path)))(Keep.left)
      .run()

  def processing(path: Path): Flow[ByteString, Lines, UniqueKillSwitch] = Flow[ByteString]
    .via(Framing.delimiter(ByteString("\n"), Int.MaxValue))
    .map(_.utf8String)
    .groupedWithin(100, 250.millis)
    .map(Lines(path, _))
    .viaMat(KillSwitches.single)(Keep.right)
}

object Parser {

  sealed trait StreamControl

  object StreamControl {

    case object Init extends StreamControl

    case object Ack extends StreamControl

    case object TailingCompleted extends StreamControl with Events

  }

  case object Ok

  sealed trait Events

  object Events {

    case class IncOffset(path: Path, offsetDelta: Int) extends Events

    case class FileChanged(newPath: Path) extends Events

    case class FileReaded(path: Path) extends Events

  }

  sealed trait State

  case class FileCursor(path: Path, offset: Long = 0l)

  object State {

    case object EmptyState extends State

    case class CurrentFile(cursor: FileCursor, control: UniqueKillSwitch) extends State

    case class CurrentWithHistory(history: Map[Path, FileCursor], current: CurrentFile) extends State

  }

  sealed trait Commands

  object Commands {

    case class Lines(path: Path, lines: Seq[String]) extends Commands

    case class ChangeFile(nrePath: Path) extends Commands

  }

}
