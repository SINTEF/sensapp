/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 * Module: net.modelbased.sensapp.backyard.echo
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
package net.modelbased.sensapp.backyard.echo

import cc.spray._
import akka.actor.{Props, ActorSystem}


class Boot(system: ActorSystem) {

  val echoModule = new EchoService {
    implicit def actorSystem = system
  }
  
  val service = system.actorOf(
    props = Props(new HttpService(echoModule.simpleService)),
    name = "echo-service"
  )
  
  val rootService = system.actorOf(
    props = Props(new RootService(service)),
    name = "spray-root-service" 
  )
  
}