/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 * Module: net.modelbased.sensapp
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
import net.modelbased.sensapp.library.system.{Service => SensAppService, URLHandler}
import net.modelbased.sensapp.library.senml.spec.{Standard => SenMLStd}
import net.modelbased.sensapp.service.registry.data._
import net.modelbased.sensapp.service.registry.data.ElementJsonProtocol._
import net.modelbased.sensapp.service.registry.data.Backend


trait RegistryService extends SensAppService {
  
  override implicit lazy val partnerName = "registry"
  
  override implicit lazy val partnersNames = List("database.raw")
    
  val service = {
    path("registry" / "sensors") {
      get { 
        parameter("flatten" ? false) { flatten =>  context =>
          val descriptors =  _registry.retrieve(List()).par
          if (flatten) {
            context complete descriptors.seq
          } else {
            val uris = descriptors map { s => URLHandler.build("/registry/sensors/"+ s.id) }
            context complete uris.seq
          }
        } 
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
            context complete URLHandler.build("/registry/sensors/" + request.id)
          }
        }
      } ~ cors("GET", "POST")
    } ~ 
    path("registry" / "sensors" / SenMLStd.NAME_VALIDATOR.r ) { name =>
      get { context =>
        ifExists(context, name, {context complete (_registry pull ("id", name)).get})
      } ~
      delete { context =>
        ifExists(context, name, {
          val sensor = _registry pull ("id", name)
          delDatabase(name)
          propagateDeletionToComposite(URLHandler.build("/registry/sensors/" + sensor))
          _registry drop sensor.get
          context complete "true"
        })
      } ~
      put {
        content(as[SensorInformation]) { info => context =>
          ifExists(context, name, {
            val sensor = (_registry pull ("id", name)).get
            val safe = SensorInformation(info.tags.filter( t => t._1 != "" ), info.updateTime, info.localization)
            sensor.infos = safe
            _registry push sensor
            context complete sensor
          })
        } ~
        content(as[DescriptionUpdate]) { request => context =>
          ifExists(context, name, {
            val sensor = (_registry pull ("id", name)).get
            sensor.description = request.description
            _registry push sensor
            context complete sensor
          })
        }
      } ~ cors("GET", "DELETE", "PUT")
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
  
  private[this] def propagateDeletionToComposite(url: String) {
    // TODO: implement me
    val _compositeRegistry = new CompositeSensorDescriptionRegistry()
    _compositeRegistry.pull(("sensors", ""))
   
  }
  
  private[this] val _registry = new SensorDescriptionRegistry()
  
  private def ifExists(context: RequestContext, id: String, lambda: => Unit) = {
    if (_registry exists ("id", id))
      lambda
    else
      context fail(StatusCodes.NotFound, "Unknown sensor [" + id + "]") 
  } 
  
}