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
package net.modelbased.sensapp.service.registry.data

import cc.spray.json._
import net.modelbased.sensapp.library.senml.spec.Standard

/**
 * Schema to be used by the underlying databased
 * @param backend the backend to be used (e.g., raw, rrdb)
 * @param template the database template to be used (backend-dependent)
 * @param baseTime the reference time associated to this sensor in the database
 */
case class Schema(val backend: String, val template: String, val baseTime: Option[Long])

/**
 * CreationRequest to declare a new sensor in SensApp
 * @param id the identifier of this sensor (must ba a valid SENML identifier)
 * @param description a description associated to this sensor
 * @param schema the database schema to be used
 */
case class CreationRequest(val id: String, val description: String, val schema: Schema) {
  
  require(id.matches(Standard.NAME_VALIDATOR), "id must be a valid SENML identifier (given: ["+id+"])")
  
  /**
   * Transform a CreationRequest into its associated SensorDescription
   * @param backend the database backend obtained from the selected database service
   */
  def toDescription(backend: Backend) = {
    val creationDate = (System.currentTimeMillis / 1000)
    val infos = SensorInformation(Map(), None, None)
    SensorDescription(this.id, this.description, backend, creationDate, infos)
  }
}

/**
 * Information about the database backend
 * @param kind the backend kind (e.g., raw, rrdb)
 * @param descriptor the URL that describes the sensor database
 * @param dataset the URL to be used to manipulate sensor data
 */
case class Backend(val kind: String, val descriptor: String, val dataset: String)

/**
 * Information about a sensor (e.g., meta-data)
 * @param tags a key-value map to store arbitrary metadata
 * @param updateRate an optional updateRate for this sensor
 * @param localization an optional localization for this sensor
 */
case class SensorInformation(
  val tags: Map[String, String],
  val updateTime: Option[Long],
  val localization: Option[Localisation]
  )

/**
 * Description of a SensApp sensor
 * @param id the sensor identified (valid SENML identifier)
 * @param description description a description associated to this sensor
 * @param backend the used backend
 * @param creationDate auto-generated field storing the creation timestamp
 * @param infos meta-data about this sensor
 */
case class SensorDescription(
  val id: String,
  var description: String,
  val backend: Backend,
  val creationDate: Long,
  var infos: SensorInformation
  )

/**
 * Localisation information for a given sensor
 * @param longitude numerical value representing the longitude
 * @param latitude numerical value representing the lattiude
 */
case class Localisation(val longitude: Double, latitude: Double)

object ElementJsonProtocol extends DefaultJsonProtocol {
  implicit val localisation = jsonFormat(Localisation, "longitude", "latitude")
  implicit val backend = jsonFormat(Backend, "kind", "descriptor", "dataset")
  implicit val infos = jsonFormat(SensorInformation, "tags", "update_time", "loc")
  implicit val sensorDescription = jsonFormat(SensorDescription, "id", "descr", "backend", "creation_date", "infos")
  implicit val schema = jsonFormat(Schema, "backend", "template", "baseTime")
  implicit val creationRequest = jsonFormat(CreationRequest, "id", "descr", "schema")
}

