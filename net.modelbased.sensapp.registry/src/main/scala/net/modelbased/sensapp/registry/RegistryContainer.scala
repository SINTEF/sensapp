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
package net.modelbased.sensapp.registry

import net.modelbased.sensapp.restful._
import net.modelbased.sensapp.registry.services._

class RegistryContainer extends Container {

  val sensorRegistryPattern = new URIPattern("/sensapp-registry/sensors/{id:string}")
  val sensorRegistryFactory = { req: String => new SensorRegistryService(sensorRegistryPattern, req) }
  
  val sensorListerPattern = new URIPattern("/sensapp-registry/sensors")
  val sensorListerFactory = { req: String => new SensorListerService(sensorListerPattern, req) }
  
  
  override val _registered = List(
      bind(sensorRegistryPattern, sensorRegistryFactory),
      bind(sensorListerPattern, sensorListerFactory))
}
