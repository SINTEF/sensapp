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
 * Module: net.modelbased.sensapp.service.notifier
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
package net.modelbased.sensapp.service.notifier.protocols

import net.modelbased.sensapp.library.senml.{MeasurementOrParameter, Root}
import net.modelbased.sensapp.service.notifier.data.Subscription
import net.modelbased.sensapp.library.system.URLHandler
import net.modelbased.sensapp.library.system._
import akka.actor.ActorSystem
import org.java_websocket.WebSocket
import net.modelbased.sensapp.library.senml.export.JsonParser
import net.modelbased.sensapp.library.ws.{ServerWebSocketClient, WsServerFactory}
import org.java_websocket.client.WebSocketClient

/**
 * Created with IntelliJ IDEA.
 * User: Jonathan
 * Date: 15/07/13
 * Time: 14:38
 */
class WsProtocol extends AbstractProtocol{
  def send(root: Root, subscription: Option[Subscription], sensor: String) {
    if (None == root.measurementsOrParameters || None == subscription)
      return

    val wsClient = WsServerFactory.myServer.getClientsById(subscription.get.id.get)
    for(i<-0 to wsClient.size()-1){
      wsClient.get(i).getWebSocket.send(JsonParser.toJson(root))
    }
  }
}
