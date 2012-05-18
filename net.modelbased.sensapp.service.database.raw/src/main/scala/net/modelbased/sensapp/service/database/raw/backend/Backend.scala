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
package net.modelbased.sensapp.service.database.raw.backend

import net.modelbased.sensapp.service.database.raw.data._
import net.modelbased.sensapp.library.senml._
import cc.spray.json._
import DataSetProtocols._

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
   * push a data set (as a SenML message) into the database
   * @param sensor the sensor identifier to be used
   * @param data the data to be pushed
   * @return a list of ignored data (that are not related to this sensor)
   */
  def push(sensor: String, data: Root): List[MeasurementOrParameter]
  
  /**
   * Retrieve **ALL** the data associated to a given sensor
   * @param sensor the sensor identifier to be used
   * @return a SenML Root object
   */
  def get(sensor: String): Root
  
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

  
  /**
   * transform a CreationRequest into the associated DataSet[X], returned as plain JSON
   * @param request the creation request to be transformed
   * @return a JSON string representing the DataSet[X] associated to the given request
   */
  /*protected def request2json(request: CreationRequest): String = {
    val json = request.schema match { 
	  case "Numerical" => DataSet[NumericalEntry](request.sensor, request.baseTime, List(),"Numerical").toJson
	  case "String"    => DataSet[StringEntry](request.sensor, request.baseTime, List(), "String").toJson
	  case "Boolean"   => DataSet[BooleanEntry](request.sensor, request.baseTime, List(), "Boolean").toJson
	  case "Summed"    => DataSet[SummedEntry](request.sensor, request.baseTime, List(), "Summed").toJson
	  case "NumericalStreamChunk" => DataSet[NumericalStreamChunkEntry](request.sensor, request.baseTime, List(), "NumericalStreamChunk").toJson
	  case _ => throw new RuntimeException("Unsuported Schema") // Cannot happen!
    }
    json.toString
  } */
  
  
  /**
   * Transform a SenML MeasurementOrParameter into a JSON string compliant with the sensor database schema
   * @param baseTime the reference time stamp (defined in the Root object)
   * @param mop the MeasurementOrParameter to be transformed
   * @return a json string representing mop as a consistent DataEntry, serialized as JSON
   */
  /*protected def data2json(baseTime: Long, mop: MeasurementOrParameter): String = {
    val delta = mop.time.get - baseTime
    val unit = mop.units.get
    val json = mop.data match {
        case FloatDataValue(f)   => NumericalEntry(delta, f, unit).toJson
        case StringDataValue(s)  => StringEntry(delta, s, unit).toJson
        case BooleanDataValue(b) => BooleanEntry(delta, b).toJson
        case SumDataValue(d,i)   => SummedEntry(delta, d, unit, i).toJson
    }
    json.toString
  }*/
 
}



