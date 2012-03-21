/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.library.system
 *
 * SensApp is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * SensApp is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General
 * Public License along with SensApp. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package net.modelbased.sensapp.library.system

import akka.config.Supervision._
import akka.actor.Supervisor
import akka.actor.Actor._
import akka.actor.ActorRef
import cc.spray._

trait System {

  def services: List[Service]
  
  def bootstrap() = {
    var actorRefs : List[ActorRef] = services map { s => actorOf(new HttpService(s.service))}
    val root = actorOf(new RootService(actorRefs.head, actorRefs.tail: _*))
    val supervisors = actorRefs map {Supervise(_,Permanent)}
    Supervisor(
      SupervisorConfig(
        OneForOneStrategy(List(classOf[Exception]), 3, 100),
        Supervise(root, Permanent) :: supervisors))
  }
  
  bootstrap
}