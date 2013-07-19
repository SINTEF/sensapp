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

/**
 * Request to create a database for a given sensor
 * @param sensor the sensor identifier to be used
 * @param baseTime the initial time stamp (seconds since EPOCH)
 * @param schema the data entry schema to be used (schema \in {"Numerical","String","Boolean", "Summed", "NumericalStreamChunk"})
 */
case class CreationRequest (val sensor: String, val baseTime: Long, val schema: String){
  require(List("Numerical","String","Boolean", "Summed", "NumericalStreamChunk").contains(schema))
}

/**
 * Request to search data in the database
 * @param sensors a sequence of relevant sensors
 * @param from lower bound of the search interval 
 * @param to upper bound of the search interval 
 */
case class SearchRequest(val sensors: Seq[String], val from: String, val to: String, val sorted: Option[String], limit: Option[Int])

/**
 * Description of the database associated to a given sensor
 * @param sensor the sensor database identifier
 * @param schema the data schema associated to this database
 * @param size the number of data stored in this database
 * @param url the URL where one can retrieve the data associated to this database 
 */
case class SensorDatabaseDescriptor(val sensor: String, val schema: String, val size: Long, val url: String)

/**
 * Spray-Json protocols used to (un)marshal the requests
 */
object RequestsProtocols extends DefaultJsonProtocol {
  implicit val creationRequest = jsonFormat(CreationRequest,"sensor", "baseTime", "schema")
  implicit val searchRequest = jsonFormat(SearchRequest, "sensors", "from", "to", "sorted", "limit")
  implicit val sensorDatabaseDescriptor = jsonFormat(SensorDatabaseDescriptor,"sensor", "schema", "size", "data_lnk")
}
