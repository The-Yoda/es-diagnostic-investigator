package org.yoda

import akka.actor.ActorSystem
import akka.stream.Supervision.Resume
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}

object Commons {

  implicit val system = ActorSystem("es-diagnoser")

  private val settings = ActorMaterializerSettings(system).withSupervisionStrategy({ t =>
    println("Exception from stream", t)
    Resume
  })

  implicit val materializer: ActorMaterializer = ActorMaterializer(settings)
}
