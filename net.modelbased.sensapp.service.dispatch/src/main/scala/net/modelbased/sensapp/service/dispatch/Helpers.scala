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

import akka.dispatch.Await
import akka.util.duration._
import cc.spray.client._
import cc.spray.json._
import cc.spray.typeconversion.DefaultUnmarshallers._
import net.modelbased.sensapp.library.system._
import net.modelbased.sensapp.library.senml._

object Dispatch extends HttpSpraySupport with io.Marshaller {
  
  def httpClientName = "dispatch-helper"
  
  def apply(partners: PartnerHandler, sensor: String, mops: Seq[MeasurementOrParameter]) {
    val (dataUrl,backend) = LocalCache(partners("registry").get, sensor)
    val data = buildRootMessage(mops)
    sendData(partners, sensor, data, dataUrl, backend)
    //notifyListeners(partners("notifier").get, sensor, data)
  }
  
  private[this] def buildRootMessage(mops: Seq[MeasurementOrParameter]): Root = {
    Root(None, None, None, None, if(mops.isEmpty) None else Some(mops))
  }
  
  private[this] def sendData(partners: PartnerHandler, sensor: String, data: Root, url: String, kind: String) {
    val key = kind match {
      case "raw" => "database.raw"
      case str => throw new RuntimeException("Unsupported backend ["+str+"]")
    }
    val db = partners(key).get
    val conduit = new HttpConduit(httpClient, db._1, db._2) {
      val pipeline = simpleRequest[Root] ~> sendReceive ~> unmarshal[String]
    }
    conduit.pipeline(Put(url,Some(data)))
      .onSuccess { case x => {
          conduit.close
          notifyListeners(partners("notifier").get, sensor, data)
        } 
      }
      .onFailure { case e: UnsuccessfulResponseException => {
          conduit.close
          system.log.info("Exception while sending data ["+url+"]: " + e.responseStatus)
          throw new RuntimeException("Unable to reach sensor database ["+url+"]")
        }
      }
  }
  
  private[this] def notifyListeners(notifier: (String, Int), sensor: String, root: Root) {
    val conduit = new HttpConduit(httpClient, notifier._1, notifier._2) {
      val pipeline = simpleRequest[Root] ~> sendReceive ~> unmarshal[String]
    }
    // Asynchronous notification
    conduit.pipeline(Put("/sensapp/notifier", Some(root)))
      .onSuccess { case x => conduit.close() }
      .onFailure { 
        case e => {
          conduit.close(); 
          val url = "http://"+notifier._1 +":"+ notifier._2 + "/notifier"
          system.log.info("Exception while notifying ["+url+"]: " + e)
        }
      }
  }
}