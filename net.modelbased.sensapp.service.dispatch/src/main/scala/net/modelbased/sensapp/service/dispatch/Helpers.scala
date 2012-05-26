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
//import cc.spray.json.DefaultJsonProtocol._
import cc.spray.typeconversion.SprayJsonSupport
import cc.spray.typeconversion.DefaultUnmarshallers._
import net.modelbased.sensapp.library.system._
import net.modelbased.sensapp.library.senml._
import net.modelbased.sensapp.library.senml.export.JsonProtocol._ 

object Dispatch extends HttpSpraySupport with SprayJsonSupport {
  
  def httpClientName = "dispatch-helper"
  
  def apply(partners: PartnerHandler, sensor: String, mops: Seq[MeasurementOrParameter]) {
    val (dataUrl, backend) = getBackend(partners("registry").get, sensor)
    val root = buildRootMessage(mops)
    sendData(partners, root, dataUrl, backend)
    notifyListeners(partners("notifier").get, sensor, root)
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
  
  private[this] def sendData(partners: PartnerHandler, data: Root, url: String, kind: String) {
    val key = kind match {
      case "raw" => "database.raw"
      case str => throw new RuntimeException("Unsupported backend ["+str+"]")
    }
    val db = partners(key).get
    val conduit = new HttpConduit(httpClient, db._1, db._2) {
      val pipeline = simpleRequest[Root] ~> sendReceive ~> unmarshal[String]
    }
    val future = conduit.pipeline(Put(url,Some(data)))
    Await.result(future, 5 seconds)
    conduit.close()
  }
  
  private[this] def notifyListeners(notifier: (String, Int), sensor: String, root: Root) {
    val conduit = new HttpConduit(httpClient, notifier._1, notifier._2) {
      val pipeline = simpleRequest[Root] ~> sendReceive ~> unmarshal[String]
    }
    // Asynchronous notification
    conduit.pipeline(Put("/notifier", Some(root)))
      .onSuccess { case x => conduit.close() }
      .onFailure { 
        case e => {
          conduit.close(); 
          val url = "http://"+notifier._1 +":"+ notifier._2 + "/notifier"
          system.log.info("Exception while notifying ["+url+"]: " + e)
        }
      }
  }
  
  
  /*
  private[this] def notifyListeners(notifier: (String, Int), sensor: String, root: Root) {
    case class Notif(s: String, hooks: List[String])
    implicit def notif = jsonFormat(Notif, "sensor", "hooks")
    val conduit = new HttpConduit(httpClient, notifier._1, notifier._2) {
      val pipeline = simpleRequest ~> sendReceive ~> unmarshal[Notif]
    }
    val future = conduit.pipeline(Get("/notification/registered/"+sensor, None))
    try {
      val n = Await.result(future, 5 seconds)
      n.hooks.par foreach { url =>
        val c = new HttpConduit(httpClient, url.split("/")(0).split(":")(0), url.split("/")(0).split(":")(1).toInt) {
          val pipe = simpleRequest[Root] ~> sendReceive ~> unmarshal[String]
        }
        val f = c.pipe(Put(url.substring(url.indexOf("/"), url.length), Some(root)))
        try { Await.result(f, 5 seconds) } 
        catch { case e: Exception => system.log.debug( url + ":" + e.toString()) }
        c.close
      }
    } catch { case e: Exception => system.log.debug( sensor + ":" + e.toString()) } // Nothing to notify, silently ignore
    conduit.close()
  }*/
}