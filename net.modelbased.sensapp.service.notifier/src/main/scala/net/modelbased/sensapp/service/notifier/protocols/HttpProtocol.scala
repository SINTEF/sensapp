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

import akka.dispatch.Await
import akka.util.duration._
import cc.spray.client._
import cc.spray.json._
import cc.spray.typeconversion.SprayJsonSupport
import cc.spray.typeconversion.DefaultUnmarshallers._
import net.modelbased.sensapp.library.system._
import net.modelbased.sensapp.library.senml._
import net.modelbased.sensapp.library.senml.export.JsonProtocol._
import net.modelbased.sensapp.service.notifier.data
import net.modelbased.sensapp.service.notifier.data.{Subscription, SubscriptionRegistry}

/**
 * Created with IntelliJ IDEA.
 * User: Jonathan
 * Date: 15/07/13
 * Time: 14:38
 */
class HttpProtocol extends AbstractProtocol with HttpSpraySupport{
  def httpClientName = "http-protocol-notifier"

  def send(root: Root, subscription: Option[Subscription], sensor: String){
    if (None == root.measurementsOrParameters || None == subscription)
      return
    subscription.get.hooks.par foreach { url =>
      val data = URLHandler.extract(url)
      val conduit = new HttpConduit(httpClient, data._1._1, data._1._2) {
        val pipeline = simpleRequest[Root] ~> sendReceive
      }
      conduit.pipeline(Put(data._2, Some(root)))
        .onSuccess { case _ => conduit.close() }
        .onFailure { case _ => conduit.close(); system.log.info("Error while notifiying ["+url+"] for sensor ["+sensor+"]")}
    }
  }
}
