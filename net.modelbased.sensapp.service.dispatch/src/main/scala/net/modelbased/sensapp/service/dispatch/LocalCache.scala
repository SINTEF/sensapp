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
 * Module: net.modelbased.sensapp.service.dispatch
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
package net.modelbased.sensapp.service.dispatch

import net.modelbased.sensapp.library.system._
import akka.dispatch.Await
import akka.util.duration._
import cc.spray.client._
import cc.spray.json._
import cc.spray.typeconversion.DefaultUnmarshallers._
import cc.spray.typeconversion.SprayJsonSupport
import cc.spray.json.DefaultJsonProtocol._

object LocalCache extends HttpSpraySupport with SprayJsonSupport {

  def httpClientName = "localcache-helper"
    
  private[this] var bindings: Map[String, (String, String)] = Map()
  
  def apply(registry: (String, Int), sensor: String): (String, String) = {
    this.bindings.get(sensor) match {
      case None => {
        val data = getBackendUrl(registry, sensor)
        bindings += (sensor -> data)
        data
      }
      case Some(data) => data
    }
  }
  
  private[this] def getBackendUrl(registry: (String, Int), sensor: String): (String, String) = {
    val conduit = new HttpConduit(httpClient, registry._1, registry._2) {
      val pipeline = simpleRequest ~> sendReceive ~> unmarshal[String]
    }
    val response = conduit.pipeline(Get("/sensapp/registry/sensors/"+sensor, None))
    val data = Await.result(response, 5 seconds).asJson
	conduit.close()
    val descr = data.asJsObject.getFields("backend")(0).asJsObject.getFields("dataset", "kind")
    (descr(0).convertTo[String], descr(1).convertTo[String])
  }
  
  
}