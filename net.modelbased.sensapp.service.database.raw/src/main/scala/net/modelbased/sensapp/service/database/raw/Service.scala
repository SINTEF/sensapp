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
import cc.spray.typeconversion.SprayJsonSupport
import net.modelbased.sensapp.library.system.{Service => SensAppService}
import net.modelbased.sensapp.library.senml.{Root => SenMLRoot}
import net.modelbased.sensapp.library.senml.export.{JsonProtocol => SenMLProtocol}
import net.modelbased.sensapp.library.senml.spec.{Standard => SenMLStd}
import net.modelbased.sensapp.service.database.raw.data._
import net.modelbased.sensapp.service.database.raw.backend.Backend
import net.modelbased.sensapp.service.database.raw.backend.impl.MongoDB
import cc.spray.http.StatusCodes

trait RawDatabaseService extends SensAppService {

  private[this] val _backend: Backend = new MongoDB()
  
  import SenMLProtocol._
  import RequestsProtocols._
  
  val service = {
    path("databases" / "raw" / "sensors") { 
      get { context => 
        val uris = _backend.content map { s => context.request.path  + "/"+ s }
        context complete uris
      } ~
      post {  
        content(as[CreationRequest]) { req => context =>
          if (_backend exists req.sensor){
            context fail (StatusCodes.Conflict, "A sensor database identified as ["+ req.sensor +"] already exists!")
          } else {
            _backend create req
            context complete(StatusCodes.Created, context.request.path  + "/"+ req.sensor )
          }
        }
      }
    } ~
    path("databases" / "raw" / "sensors" / SenMLStd.NAME_VALIDATOR.r ) { name => 
      get { context => 
        ifExists(context, name, {
          val description = _backend describe(name, "/databases/raw/data/")
          context complete description
        })
      } ~
      delete { context => 
        context complete (_backend delete name).toString
      }
    } ~
    path("databases" / "raw" / "data" / SenMLStd.NAME_VALIDATOR.r ) { name => 
      get { context =>
       ifExists(context, name, {
          val dataset = _backend get(name)
          context complete dataset
        })
      } ~
      post { context => 
        context.fail(new RuntimeException("Not yet implemented"))
      } ~
      put { 
        content(as[SenMLRoot]) { raw => context =>
          context complete (_backend push (name, raw))
        }
      }
    }
  }
 
  private def ifExists(context: RequestContext, name: String, lambda: => Unit) = {
    if (_backend exists name)
      lambda
    else
      context fail(StatusCodes.NotFound, "Unknown sensor database [" + name + "]") 
  } 
  
  
  /*{
    path("databases" / "raw" / "sensors") {
      get { context =>
        val uris = _backend.content map { s => context.request.path  + "/"+ s }
        context complete uris
      } ~
      post {
        content(as[CreationRequest]) { req => context =>
          if (_backend exists req.sensor){
            context fail (StatusCodes.Conflict, "A sensor database identified as ["+ req.sensor +"] already exists!")
          } else {
            _backend create req
            context complete(StatusCodes.Created, context.request.path  + "/"+ req.sensor )
          }
        }
      }
    } ~
    path("databases" / "raw" / "sensors" / SenMLStd.NAME_VALIDATOR.r ) { name => 
      get { context => 
        if (_backend exists name) { 
          context complete (_backend describe (name,"/databases/raw/data/")) 
        } else { 
          context fail(StatusCodes.NotFound, "Unknown sensor database [" + name + "]") 
        }
      } ~
      delete { context =>
        if (_backend exists name) { 
          context complete "" + (_backend delete name) 
        } else { 
          context fail(StatusCodes.NotFound, "Unknown sensor database [" + name + "]") 
        }
      } ~
      put {
        content(as[SenMLRoot]) { root => context => 
          println("fooba44r")
          if (_backend exists name) { 
            println("foobargeek")
            _backend push(name, root)
          } else {
            context fail(StatusCodes.NotFound, "Unknown sensor database [" + name + "]") 
          }
        }
      } 
    } ~
    path("databases" / "raw" / "data" / SenMLStd.NAME_VALIDATOR.r) { name =>
      get { context =>
        if (_backend exists name) { 
          context complete (_backend get name)
        } else {
          context fail(StatusCodes.NotFound, "Unknown sensor database [" + name + "]") 
        }
      } ~
      post {
        content(as[SensorDataRequest]) { req => context =>
        
        }
      } 
    } 
  }*/
}