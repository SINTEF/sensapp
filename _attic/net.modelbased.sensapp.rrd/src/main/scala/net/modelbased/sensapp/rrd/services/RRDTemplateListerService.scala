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
package net.modelbased.sensapp.rrd.services

import net.modelbased.sensapp.restful._
import net.modelbased.sensapp.rrd.datamodel._
import akka.http._
import javax.ws.rs.core.MediaType

/**
 * List all registered sensors in the registry
 */
class RRDTemplateListerService(p: URIPattern, r: String) extends ResourceHandler(p,r) {
 
  override val _bindings = Map("GET" -> { getRRDTemplateList(_) })
  
  /**
   * Return a list of URLs to the registered sensors
   */
  private def getRRDTemplateList(req: RequestMethod): Boolean = {
    val _registry = new RRDTemplateRegistry()
    val jsonList = _registry.retrieve(List()) map { "\"" + req.request.getRequestURL() +"/" + _.id + "\""}
    req.response.setContentType(MediaType.APPLICATION_JSON)
    req OK "[" + (jsonList mkString ",") + "]"
  } 
  
}