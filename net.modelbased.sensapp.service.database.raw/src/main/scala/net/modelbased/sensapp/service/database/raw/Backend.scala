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
package net.modelbased.sensapp.service.database.raw

import net.modelbased.sensapp.service.database.raw.data._
import net.modelbased.sensapp.library.senml._
import cc.spray.json._
import DataSetProtocols._

abstract trait Backend {

  def content: List[String]
  
  def exists(sensor: String): Boolean
  
  def create(request: CreationRequest): Boolean
  
  def describe(sensor: String): Option[SensorDatabaseDescriptor]
  
  def delete(sensor: String): Boolean
 
  def push(sensor: String, dataset: List[MeasurementOrParameter]): List[MeasurementOrParameter]
 
  def getAll(sensor: String): String = null
  
  protected def request2json(request: CreationRequest): String = {
    val json = request.schema match { 
	  case "Numerical" => DataSet[NumericalEntry](request.sensor, request.baseTime, List()).toJson
	  case "String"    => DataSet[StringEntry](request.sensor, request.baseTime, List()).toJson
	  case "Boolean"   => DataSet[BooleanEntry](request.sensor, request.baseTime, List()).toJson
	  case "Summed"    => DataSet[SummedEntry](request.sensor, request.baseTime, List()).toJson
	  case "NumericalStreamChunk" => DataSet[NumericalStreamChunkEntry](request.sensor, request.baseTime, List()).toJson
	  case _ => throw new RuntimeException("Unsuported Schema") // Cannot happen!
    }
    json.toString
  }
  
  protected def data2json(baseTime: Long, mop: MeasurementOrParameter): String = {
    val delta = mop.time.get - baseTime
    val unit = mop.units.get
    val json = mop.data match {
        case FloatDataValue(f)   => NumericalEntry(delta, f, unit).toJson
        case StringDataValue(s)  => StringEntry(delta, s, unit).toJson
        case BooleanDataValue(b) => BooleanEntry(delta, b).toJson
        case SumDataValue(d,i)   => SummedEntry(delta, d, unit, i).toJson
    }
    json.toString
  }
  
}



