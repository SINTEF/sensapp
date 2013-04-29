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
package net.modelbased.sensapp.registry.services

import net.modelbased.sensapp.restful._
import net.modelbased.sensapp.registry.datamodel._
import akka.http._
import javax.ws.rs.core.MediaType

/**
 * The service that exposes a SensorRegistry as a RESTful artefact
 * 
 * @author Sebastien Mosser
 */
class SensorRegistryService(p: URIPattern, r: String) extends ResourceHandler(p,r) {
  
  // the internal registry
  private val _registry = new SensorRegistry()
  
  // The bindings expected as a ResourceHandler 
  override val _bindings = Map("GET" -> { getSensor(_) },
		  					   "PUT" -> { updateSensor(_) },
		  					   "DELETE" -> { delSensor(_) })
  
  /**
   * Retrieve a sensor from the registry, exposed as JSON
   * 
   * <strong>Remark</strong>: A 404 status is returned if there is no sensor available
   * 
   * @param req the received request
   */
  private def getSensor(req: RequestMethod): Boolean = { 
    val identifier = _params("id")
    req.response.setContentType(MediaType.APPLICATION_JSON)
    _registry pull ("id", identifier) match {
      case Some(sensor) => req OK _registry.toJSON(sensor)
      case None => req NotFound ("Sensor ["+identifier+"] not found")
    }  
  }
  
  
  /**
   * Update an existing sensor
   * @param req the received request
   */
  private def updateSensor(req: RequestMethod): Boolean = { 
    val identifier = _params("id")
    val json = req.getParameterOrElse("descriptor", _ => "{}")
    val sensor = _registry.fromJSON(json)
    if(sensor.id != identifier) {
      req Conflict "Descriptor id ["+sensor.id+"] does not match URL ones ["+identifier+"]"
    } else {
      _registry.pull(_registry.identify(sensor)) match {
        case None => req NotFound "Sensor ["+sensor.id+"] does not exist"
        case Some(_) => {
          _registry push sensor
          req OK "Sensor descriptor ["+sensor.id+"] updated"
        }
      }
    }
  }
 
  /**
   * Delete a Sensor
   * @param req the received request
   */
  private def delSensor(req: RequestMethod): Boolean = { 
    val identifier = _params("id")
    _registry pull ("id", identifier) match {
      case Some(sensor) => {
        _registry drop sensor
        req OK "deleted"
      }
      case None => req NotFound ("Sensor ["+identifier+"] not found")
    }  
  }
}