/**
 * ====
 *     This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 *     Copyright (C) 2011-  SINTEF ICT
 *     Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 *     Module: net.modelbased.sensapp
 *
 *     SensApp is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as
 *     published by the Free Software Foundation, either version 3 of
 *     the License, or (at your option) any later version.
 *
 *     SensApp is distributed in the hope that it will be useful, but
 *     WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General
 *     Public License along with SensApp. If not, see
 *     <http://www.gnu.org/licenses/>.
 * ====
 *
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: SINTEF ICT <nicolas.ferry@sintef.no>
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


import akka.actor.{Props, ActorSystem}
import cc.spray._
import org.java_websocket.drafts.Draft_17
import net.modelbased.sensapp.library.ws.WsServerFactory

/**
 * Initialize a SensApp System (register the actors, ...)
 * 
 * @remark the user must implement the "services" method
 * @author Sebastien Mosser
 */ 
trait System {

  val system: ActorSystem
  
  /**
   * The list of SensApp service to be used in this system
   */
  def services: List[Service]
  
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
  println(headers)  
  
  val actors = (services.par map { s: Service =>
      val ref = system.actorOf(props = Props(new HttpService(s.wrappedService)), name = s.partnerName)
      system.log.info("Service {} -> {}", Array(s.partnerName, ref.toString))
      ref
  }).seq
 
  val rootService = system.actorOf(
        props = Props(new RootService(actors.head, actors.tail: _*)),
        name = "spray-root-service")
  system.log.info("RootService -> {}", Array(rootService.toString))

  var webSocketServer = WsServerFactory.makeServer(9000, new Draft_17)
  webSocketServer.start()
 
  system.registerOnTermination(println("Shutting down SensApp"))
  
}