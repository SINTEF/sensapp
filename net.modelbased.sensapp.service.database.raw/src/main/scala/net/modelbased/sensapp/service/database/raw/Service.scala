/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.service.database.raw
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
package net.modelbased.sensapp.service.database.raw

import cc.spray._
import cc.spray.http._
import cc.spray.json._
import cc.spray.directives._
import cc.spray.typeconversion.SprayJsonSupport
// Application specific:
import net.modelbased.sensapp.library.system.{Service => SensAppService} 
import net.modelbased.sensapp.library.senml.{Root => SenMLRoot, Standard => SenMLStd, JsonProtocol}
import net.modelbased.sensapp.service.database.raw.data._

trait RawDatabaseService extends SensAppService {

  println("XXXXX")
  
  import DataSetProtocols._ 
  import RequestsProtocols._
  import JsonProtocol._
  
  private[this] val _backend: Backend = new MongoDB()
  
  val service = {
    path("databases" / "raw" / "sensors") {
      get { context =>
        println(_backend)
        val uris = _backend.content map { s => context.request.path  + "/"+ s }
        context complete uris
      } ~
      post {
        content(as[CreationRequest]) { req => context =>
          if (_backend exists req.sensor){
            context fail (StatusCodes.Conflict, "A sensor database identified as ["+ req.sensor +"] already exists!")
          } else {
            context complete(StatusCodes.Created, context.request.path  + "/"+ req.sensor )
          }
        }
      }
    } ~
    path("databases" / "raw" / "sensors" / SenMLStd.NAME_VALIDATOR.r) { name =>
      get { context =>
        handle(context, name, { s => context complete(_backend describe s) })
      } ~
      delete { context =>
        handle(context, name, { s => _backend delete s ; context.complete("true")})
      } ~
      put {
        content(as[SenMLRoot]) { root => context => 
          val canonised: SenMLRoot = root
          handle(context, name, { s => _backend push(s,canonised.measurementsOrParameters); context.complete("true") })
        }
      }
    } ~
    path("databases" / "raw" / "sensors" / SenMLStd.NAME_VALIDATOR.r / "content") { name =>
      get { context =>
        handle(context, name, { s => context complete(_backend getAll s) })
      } ~
      post {
        content(as[SensorDataRequest]) { req => context =>
          context complete(_backend getAll name)
        }
      }
    }
  }
  
  private def handle(ctx: RequestContext, sensor: String, action: String => Unit) = {
    if (_backend exists sensor)
      ctx fail(StatusCodes.NotFound, "Unknown sensor database ["+sensor+"]")
    else 
      action(sensor)
  }
}