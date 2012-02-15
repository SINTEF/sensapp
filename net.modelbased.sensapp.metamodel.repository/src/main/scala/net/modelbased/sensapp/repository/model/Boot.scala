package net.modelbased.sensapp.repository.model

import akka.config.Supervision._
import akka.actor.Supervisor
import akka.actor.Actor._
import cc.spray._

class Boot {
  
  val lister = new ModelLister {}
  val repository = new ModelRepository {}

  val listerService = actorOf(new HttpService(lister.service))
  val repositoryService = actorOf(new HttpService(repository.service))
  val rootService = actorOf(new RootService(listerService, repositoryService))

  Supervisor(
    SupervisorConfig(
      OneForOneStrategy(List(classOf[Exception]), 3, 100),
      List(
        Supervise(listerService, Permanent),
        Supervise(repositoryService, Permanent),
        Supervise(rootService, Permanent)
      )
    )
  )
}