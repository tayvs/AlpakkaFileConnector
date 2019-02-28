package test.alpakka.file.connector

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import test.alpakka.file.connectors.Parser

object ParserTest extends App {

  val system = ActorSystem("Parser")

  var parser = system.actorOf(Props(new Parser()))
  TimeUnit.SECONDS.sleep(10)
  system.stop(parser)

  TimeUnit.SECONDS.sleep(5)

  parser = system.actorOf(Props(new Parser()))
  TimeUnit.SECONDS.sleep(10)
  system.stop(parser)

}
