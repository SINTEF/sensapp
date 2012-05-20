/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.service.registry
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
package net.modelbased.sensapp.service.registry

import cc.spray._
import cc.spray.http._
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import cc.spray.directives._
import cc.spray.typeconversion.SprayJsonSupport
import net.modelbased.sensapp.library.system.{Service => SensAppService}
import net.modelbased.sensapp.library.senml.spec.{Standard => SenMLStd}
import net.modelbased.sensapp.service.registry.data._
import net.modelbased.sensapp.service.registry.data.ElementJsonProtocol._
import net.modelbased.sensapp.service.registry.data.Backend


trait Service extends SensAppService {
  
  override val name = "registry"
  
  val service = {
    path("registry" / "sensors") {
      get { context =>
        val list = _registry.retrieve(List()).par map {
          context.request.path  + "/"+ _.id
        }
        context complete list.seq
      } ~ 
      post {
        content(as[CreationRequest]) { request => context =>
          if (_registry exists ("id", request.id)){
            context fail (StatusCodes.Conflict, "A SensorDescription identified as ["+ request.id +"] already exists!")
          } else {
            // Create the database
            val backend = createDatabase(request.id, request.schema)
            // Store the descriptor
            _registry push (request.toDescription(backend))
            context complete (context.request.path  + "/" + request.id)
          }
        }
      }
    } ~ 
    path("registry" / "sensors" / SenMLStd.NAME_VALIDATOR.r ) { name =>
      get { context =>
        ifExists(context, name, {context complete (_registry pull ("id", name)).get})
      } ~
      delete { context =>
        ifExists(context, name, {
          val sensor = _registry pull ("id", name)
          delDatabase(name)
          _registry drop sensor.get
          context complete "true"
        })
      } ~
      put {
        content(as[SensorInformation]) { info => context =>
          ifExists(context, name, {
            val sensor = (_registry pull ("id", name)).get
            sensor.infos = info
            _registry push sensor
            context complete sensor
          })
        }
      }
    }
  }
  
  private[this] def createDatabase(id: String, schema: Schema): Backend = {
    val helper = BackendHelper(schema)
    val urls = helper.createDatabase(id, schema, partners)
    Backend(schema.backend, urls._1, urls._2) 
  }
  
  private[this] def delDatabase(id: String) = {
    val backend = (_registry pull ("id", id)).get.backend
    val helper = BackendHelper(backend)
    helper.deleteDatabase(backend, partners)
  }
  
  private[this] val _registry = new SensorDescriptionRegistry()
  
  private def ifExists(context: RequestContext, id: String, lambda: => Unit) = {
    if (_registry exists ("id", id))
      lambda
    else
      context fail(StatusCodes.NotFound, "Unknown sensor [" + id + "]") 
  } 
  
}