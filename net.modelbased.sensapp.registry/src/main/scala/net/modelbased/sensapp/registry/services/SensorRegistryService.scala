/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.registry
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
  override val _bindings = Map("GET"  -> { getSensor(_) }, 
		  					   "POST" -> { addSensor(_) })
  
  /**
   * Retrieve a sensor from the registry, exposed as JSON
   * 
   * <strong>Remark</strong>: A 404 status is returned if there is no sensor available
   * 
   * @param req the received request
   */
  private def getSensor(req: RequestMethod) = { 
    val identifier = _params("id")
    req.response.setContentType(MediaType.APPLICATION_JSON)
    _registry pull ("id", identifier) match {
      case Some(sensor) => req OK _registry.toJSON(sensor)
      case None => req NotFound ("Sensor ["+identifier+"] not found")
    }  
  }
  
  /**
   * Add a sensor into the registry, provided as JSON
   * 
   * <strong>Remark</strong>: 
   * <ul>
   * <li>The sensor is described using JSON</li>
   * <li>The description is provided through the <code>descriptor</code> parameter</li>
   * <li> A conflict (409) is returned if the descriptot ID does not match the URL one
   * </ul>
   */
  private def addSensor(req: RequestMethod) = {
    val json = req.getParameterOrElse("descriptor", _ => "{}")
    val sensor = _registry.fromJSON(json)
    req.response.setContentType(MediaType.TEXT_PLAIN)
    if (_params("id") != sensor.id){
     req Conflict ("Url refers to id ["+_params("id")+"], but descriptor uses ["+sensor.id+"]")
    } else {
      _registry push sensor
      req OK "true"
    }
  }
}