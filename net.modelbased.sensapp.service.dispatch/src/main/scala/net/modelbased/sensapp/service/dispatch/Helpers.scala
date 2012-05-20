/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
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

import akka.dispatch.Await
import akka.util.duration._
import cc.spray.client._
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import cc.spray.typeconversion.SprayJsonSupport
import cc.spray.typeconversion.DefaultUnmarshallers._
import net.modelbased.sensapp.library.system.{HttpSpraySupport, PartnerHandler}
import net.modelbased.sensapp.library.senml._
import net.modelbased.sensapp.library.senml.export.JsonProtocol._ 

object Dispatch extends HttpSpraySupport with SprayJsonSupport {
  
  def httpClientName = "dispatch-helper"
  
  def apply(partners: PartnerHandler, sensor: String, mops: Seq[MeasurementOrParameter]) {
    val (dataUrl, backend) = getBackend(partners("registry"), sensor)
    val root = buildRootMessage(mops)
    sendData(partners, root, dataUrl, backend)
  }
  
  private[this] def sendData(partners: PartnerHandler, data: Root, url: String, kind: String) {
    val key = kind match {
      case "raw" => "database.raw"
      case str => throw new RuntimeException("Unsupported backend ["+str+"]")
    }
    val partner = partners(key)
    val conduit = new HttpConduit(httpClient, partner._1, partner._2) {
      val pipeline = simpleRequest[Root] ~> sendReceive ~> unmarshal[String]
    }
    val future = conduit.pipeline(Put(url,Some(data)))
    Await.result(future, 5 seconds)
    conduit.close()
  }
  
  private[this] def buildRootMessage(mops: Seq[MeasurementOrParameter]): Root = {
    Root(None, None, None, None, Some(mops))
  }
  
  private[this] def getBackend(registry: (String, Int), sensor: String ): (String, String) = {
    val conduit = new HttpConduit(httpClient, registry._1, registry._2) {
      val pipeline = simpleRequest ~> sendReceive ~> unmarshal[String]
    }
    val response = conduit.pipeline(Get("/registry/sensors/"+sensor, None))
    val data = Await.result(response, 5 seconds).asJson
	conduit.close()
    val descr = data.asJsObject.getFields("backend")(0).asJsObject.getFields("dataset", "kind")
    (descr(0).convertTo[String], descr(1).convertTo[String]) 
  }  
}