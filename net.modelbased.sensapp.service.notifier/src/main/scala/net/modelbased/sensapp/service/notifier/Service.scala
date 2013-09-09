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
package net.modelbased.sensapp.service.notifier

import cc.spray._
import cc.spray.http._
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import cc.spray.directives._
import cc.spray.typeconversion.SprayJsonSupport
import net.modelbased.sensapp.service.notifier.data.{Subscription, SubscriptionRegistry }
import net.modelbased.sensapp.service.notifier.data.SubscriptionJsonProtocol.format
import net.modelbased.sensapp.library.senml.Root
import net.modelbased.sensapp.library.senml.export.{JsonProtocol => SenMLProtocol}
import net.modelbased.sensapp.library.senml.spec.Standard
import net.modelbased.sensapp.library.system.{Service => SensAppService} 
import net.modelbased.sensapp.library.system.URLHandler
import java.util.UUID

trait Service extends SensAppService {
  
  import SenMLProtocol._
  
  override implicit lazy val partnerName = "notifier"
    
  val service = {
    path("notifier") {
      detach {
        put {
          content(as[Root]) {root => context =>
            root.dispatch.par foreach {
              case (sensor, data) => Helper.doNotify(data, sensor, _registry) 
            }
            context complete "done"
          }
        }
      } ~ cors("PUT")
    } ~ 
    path("notification" / "registered" ) {
      get { 
        parameters("flatten" ? false, "protocol" ? "all") { (flatten, protocol) =>  context =>
          val data = (_registry retrieve(List()))
          if(protocol.equals("ws") && flatten){
            context complete (data filter(sub => {sub.protocol.isDefined && sub.protocol.get == "ws"}))
          } else if(protocol.equals("ws")){
            context complete (data filter(sub => {sub.protocol.isDefined && sub.protocol.get == "ws"}))
              .map { s => URLHandler.build("/notification/registered/" + s.sensor) }

          } else if (protocol.equals("http") && flatten) {
            context complete (data filter(sub => {!sub.protocol.isDefined}))
          } else if (protocol.equals("http")){
            context complete (data filter(sub => {!sub.protocol.isDefined}))
              .map { s => URLHandler.build("/notification/registered/" + s.sensor) }

          } else if (flatten) {
            context complete data
          } else {
            context complete (data map { s => URLHandler.build("/notification/registered/" + s.sensor)})
          }
        }
      } ~
      post {
        content(as[Subscription]) { subscription => context =>
          if (_registry exists ("sensor", subscription.sensor)){
            context fail (StatusCodes.Conflict, "A Subscription identified by ["+ subscription.sensor +"] already exists!")
          } else {
            /*subscription.protocol.foreach(p => {
              if(p == "ws" && !subscription.id.isDefined)
                subscription.id=Option(UUID.randomUUID().toString)
            })*/
            _registry push subscription
            context complete (StatusCodes.Created, URLHandler.build("/notification/registered/" + subscription.sensor))
          }
        }
      } ~ cors("GET", "POST")
    } ~
    path("notification" / "registered" / Standard.NAME_VALIDATOR.r) { name =>
      get { context =>
        ifExists(context, name, {context complete (_registry pull ("sensor", name)).get})
      } ~
      delete { context =>
        ifExists(context, name, {  
          val subscr = (_registry pull ("sensor", name)).get
          _registry drop subscr 
          context complete "true"
        })
      } ~
      put {
        content(as[Subscription]) { subscription => context => 
          if (subscription.sensor != name) {
            context fail(StatusCodes.Conflict, "Request content does not match URL for update")
          } else {
            ifExists(context, name, { _registry push(subscription); context complete subscription })
	      } 
        }
      } ~ cors("GET", "PUT", "DELETE")
    }
  }
  
  private[this] val _registry = new SubscriptionRegistry()
  
  private def ifExists(context: RequestContext, id: String, lambda: => Unit) = {
    if (_registry exists ("sensor", id))
      lambda
    else
      context fail(StatusCodes.NotFound, "Unknown sensor identifier for notification [" + id + "]") 
  } 
  
}