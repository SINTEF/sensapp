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
import net.modelbased.sensapp.library.system.{Service => SensAppService, URLHandler}
import net.modelbased.sensapp.library.senml.spec.{Standard => SenMLStd}
import net.modelbased.sensapp.service.registry.data._
import net.modelbased.sensapp.service.registry.data.ElementJsonProtocol._


trait CompositeRegistryService extends SensAppService {
  
  override implicit lazy val partnerName = "registry.composite"
    
  val service = {
    path("registry" / "composite" / "sensors") {
      get { 
        parameter("flatten" ? false) { flatten =>  context =>
          val descriptors =  _registry.retrieve(List()).par
          if (flatten) {
            context complete descriptors.seq
          } else {
            val uris = descriptors map { s => URLHandler.build("/registry/composite/sensors/"+ s.id) }
            context complete uris.seq
          }
        } 
      } ~ 
      post {
        content(as[CompositeSensorDescription]) { request => context =>
          if (_registry exists ("id", request.id)){
            context fail (StatusCodes.Conflict, "A CompositeSensorDescription identified as ["+ request.id +"] already exists!")
          } else {
            _registry push (request)
            context complete URLHandler.build("/registry/composite/sensors/"+ request.id)
          }
        }
      } ~ cors("GET", "POST")
    } ~ 
    path("registry" / "composite" / "sensors" / SenMLStd.NAME_VALIDATOR.r ) { name =>
      get { context =>
        ifExists(context, name, {context complete (_registry pull ("id", name)).get})
      } ~
      delete { context =>
        ifExists(context, name, {
          val sensor = _registry pull ("id", name)
          _registry drop sensor.get
          context complete "true"
        })
      } ~
      put {
        content(as[SensorList]) { sensorList => context =>
          ifExists(context, name, {
            val sensor = (_registry pull ("id", name)).get
            sensor.sensors = sensorList.sensors
            _registry push sensor
            context complete sensor
          })
        } ~
        content(as[SensorTags]) { tags => context =>
          ifExists(context, name, {
            val sensor = (_registry pull ("id", name)).get
            sensor.tags = Some(tags.tags.filter( t => t._1 != "" ))
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
  
  private[this] val _registry = new CompositeSensorDescriptionRegistry()
  
  private def ifExists(context: RequestContext, id: String, lambda: => Unit) = {
    if (_registry exists ("id", id))
      lambda
    else
      context fail(StatusCodes.NotFound, "Unknown sensor [" + id + "]") 
  } 
  
}