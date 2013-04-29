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
 * List all registered sensors in the registry
 */
class SensorListerService(p: URIPattern, r: String) extends ResourceHandler(p,r) {
 
  override val _bindings = Map("GET"  -> { getSensorList(_) },
		  				       "POST" -> { addSensor(_) } )
  
  private val _registry = new SensorRegistry()
  
  /**
   * Return a list of URLs to the registered sensors
   */
  private def getSensorList(req: RequestMethod): Boolean = {
    val jsonList = _registry.retrieve(List()) map { "\"" + buildSensorURI(req,_) + "\""}
    req.response.setContentType(MediaType.APPLICATION_JSON)
    req OK "[" + (jsonList mkString ", ") + "]"
  } 
  
  /**
   * Add a sensor into the registry, provided as JSON
   * 
   * <strong>Remark</strong>: 
   * <ul>
   * <li>The sensor is described using JSON</li>
   * <li>The description is provided through the <code>descriptor</code> parameter</li>
   * <li> A conflict (409) is returned if the descriptor ID is already used
   * <li> A Created (201) is returned, and location is set to the sensor URI
   * </ul>
   */
  private def addSensor(req: RequestMethod) = {
    req.response.setContentType(MediaType.TEXT_PLAIN)
    val json = req.getParameterOrElse("descriptor", _ => "{}")
    val sensor = _registry.fromJSON(json)
    _registry.pull(_registry.identify(sensor)) match {
      case Some(s) => req Conflict ("Sensor id is already used ["+sensor.id+"]")
      case None => {
        _registry push sensor
        req.response.setHeader("Location", buildSensorURI(req,sensor))
        req Created "true"
      }
    }
  }

  /**
   * Build the URI associated to a sensor, using the requestURL as prefix
   * 
   * @param req the received request
   * @param s the sensor
   */
  private def buildSensorURI(req: RequestMethod, s: Sensor): String = {
    req.request.getRequestURL() +"/" + s.id 
  }
}