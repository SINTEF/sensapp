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

/**
 * Initialize a SensApp System (register the actors, ...)
 * 
 * @remark the user must implement the "services" method
 * @author Sebastien Mosser
 */ 
trait System extends HttpSpraySupport {

  /**
   * The list of SensApp service to be used in this system
   */
  def services: List[Service]
  
  /**
   * bootstrap the Spray backend (Akka layer)
   */
  private[this] def bootstrap() = {
    var actorRefs : List[ActorRef] = services map { s => actorOf(new HttpService(s.service))}
    val root = actorOf(new RootService(actorRefs.head, actorRefs.tail: _*))
    val supervisors = actorRefs map {Supervise(_,Permanent)}
    Supervisor(
      SupervisorConfig(
        OneForOneStrategy(List(classOf[Exception]), 3, 100),
        Supervise(root, Permanent) :: supervisors))
  }
  
  // Headers to be printed while starting up SensApp
  private[this] val headers = """
                     _____                 ___                               
                    / ___/___  ____  _____/   |  ____  ____                  
                    \__ \/ _ \/ __ \/ ___/ /| | / __ \/ __ \                 
                   ___/ /  __/ / / (__  ) ___ |/ /_/ / /_/ /                 
                  /____/\___/_/ /_/____/_/  |_/ .___/ .___/                  
                                             /_/   /_/                       

Copyright (C) 2011-  SINTEF ICT ~ NSS Department ~ MOD Group
This program comes with ABSOLUTELY NO WARRANTY; This is free software, 
and you are welcome to redistribute it under certain conditions; 

License: GNU Lesser General Public License, v3
Website: http://sensapp.modelbased.net 
Contact: Sebastien Mosser <Sebastien.Mosser@sintef.no>
"""
    
  bootstrap
  load
  println(headers)
}