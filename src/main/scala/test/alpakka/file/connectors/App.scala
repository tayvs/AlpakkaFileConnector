package test.alpakka.file.connectors

import akka.actor.{ActorRef, ActorSystem, Props}

object App extends App {

  val system = ActorSystem("Parser")
  val actor: ActorRef = system.actorOf(Props(new Parser), "parser")


}
