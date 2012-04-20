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
package net.modelbased.sensapp.service.database.raw.data

import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol

case class CreationRequest (val sensor: String, val baseTime: Long, val schema: String){
  require(List("Numerical","String","Boolean", "Summed", "NumericalStreamChunk").contains(schema))
}

case class SensorDatabaseDescriptor(val sensor: String, val schema: String, val size: Long, val url: String)

case class SensorDataRequest(val start: Option[Long], val end: Option[Long])

object RequestsProtocols extends DefaultJsonProtocol {
  implicit val creationRequest = jsonFormat(CreationRequest,"sensor", "baseTime", "schema")
  implicit val sensorDatabaseDescriptor = jsonFormat(SensorDatabaseDescriptor,"sensor", "schema", "size", "data_lnk")
  implicit val sensorDataRequest = jsonFormat(SensorDataRequest, "start", "end")
}
