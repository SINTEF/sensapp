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
package net.modelbased.sensapp.service.database.raw.backend

import net.modelbased.sensapp.service.database.raw.data._
import net.modelbased.sensapp.library.senml._
import cc.spray.json._

/**
 * Trait to define the "interface" a raw backend database must implement
 * @author mosser
 */
abstract trait Backend extends BackendStructure {
  
  /**
   * retrieve the sensor databases stored in this backend
   * @return a list of sensor database identifiers
   */
  def content: List[String]  
  
  /**
   * check if a given sensor database exists
   * @param sensor: the identifier to be checked
   * @param return true if the sensor exists, false else where
   */
  def exists(sensor: String): Boolean  
  
  /**
   * Create a database according to a given creation request
   * @param request the request to be executed
   * @return true if the request was completed, false elsewhere
   */
  def create(request: CreationRequest): Boolean
  
  /**
   * Describe a given sensor database
   * @param sensor the sensor identifier
   * @param prefix the URL prefix associated to the data 
   * @return None if sensor does not exists, a SensorDatabaseDescritpor elsewhere
   */
  def describe(sensor: String, prefix: String): Option[SensorDatabaseDescriptor]
  
  /**
   * __Permanentely__ delete a sensor database
   * @param sensor the sensor identifier to be deleted
   * @return true if the database was successfully deleted, false elsewhere
   * 
   */
  def delete(sensor: String): Boolean
  
  /**
   * push a data set (as a SenML message) into the database. If a data for the same time stamp already exists,
   * the pushed data is considered as an update of the legacy one (i.e., the legacy one is replaced)
   * @param sensor the sensor identifier to be used
   * @param data the data to be pushed
   * @return a list of ignored data (that are not related to this sensor)
   */
  def push(sensor: String, data: Root): Seq[MeasurementOrParameter]
  
  /**
   * Import a dataset, bypassing all control mechanisms introduced by the usual "push"
   * @param data the data set to be loaded (constraints: unique timestamp)
   */
  def importer(data: Root)

  
  /**
   * Retrieve the data associated to a given sensor for a given interval
   * @param sensor the sensor identifier to be used
   * @param from lower bound time stamp (seconds since EPOCH) 
   * @param to upper bound time stamp (seconds since EPOCH) 
   * @param limit the maximum number of measure
   * @return a SenML Root object
   */
  def get(sensor: String, from: Long, to: Long, sorted: String, limit: Int): Root
  
  /**
   * Retrieve the data associated to a set of sensors for a given interval
   * @param sensors the sensor identifiers to be used
   * @param from lower bound time stamp (seconds since EPOCH) 
   * @param to upper bound time stamp (seconds since EPOCH) 
   * @param limit the maximum number of measure
   * @return a SenML Root object
   */
  def get(sensor: Seq[String], from: Long, to: Long, sorted: String, limit: Int): Root
  
  /**
   * return the raw database schema associated to a given sensor
   * 
   * schema \in {"Numerical", "String", "Boolean", "Summed", "NumericalStreamChunk"}
   */
  def getSchema(sensor: String): String
  
  /**
   * List of supported schemas
   */
  def schemas: List[String] = RawSchemas.values.toList map { _.toString } 
}



