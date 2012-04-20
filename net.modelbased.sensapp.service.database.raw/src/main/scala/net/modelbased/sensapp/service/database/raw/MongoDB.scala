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

import cc.spray.json._
import net.modelbased.sensapp.service.database.raw.data._
import net.modelbased.sensapp.library.senml.{Root => SenML, MeasurementOrParameter}
import com.mongodb.casbah.Imports._
import com.mongodb.util.JSON
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.commons.MongoDBList

class MongoDB extends Backend {

  def content: List[String] = {
    val data = collection.find(MongoDBObject.empty, MongoDBObject("s" -> 1))
    val it = data map {e => e.getAs[String]("s").get }
    it.toList
  }
  
  def exists(sensor: String): Boolean = {
    val data = collection.findOne(MongoDBObject("s" -> sensor), MongoDBObject("s" -> 1))
    data != None
  }
  
  def create(request: CreationRequest): Boolean = {
    collection += request2json(request)
    true
  }
  
  def describe(sensor: String, prefix: String): Option[SensorDatabaseDescriptor] = {
    collection.findOne(MongoDBObject("s" -> sensor)) match {
      case None => None
      case Some(dbObj) => {
        val schema = getSchema(sensor)
        val size = dbObj.get("e").asInstanceOf[BasicDBList].size
        val obj = SensorDatabaseDescriptor(dbObj.getAs[String]("s").get,schema,size, prefix+sensor)
        Some(obj)
      }
    }
  }
  
  def delete(sensor: String): Boolean = {
    collection.findOne(MongoDBObject("s" -> sensor)) match {
      case None => false
      case Some(dbObj) => { collection -= dbObj; true }
    }
  }
  
  def get(sensor: String): SenML = {
    import DataSetProtocols._
    require(exists(sensor), "Unknown sensor")
    val obj = collection.findOne(MongoDBObject("s" -> sensor)).get
    val r = getSchema(sensor) match { 
      case "Numerical" => obj.toString.asJson.convertTo[DataSet[NumericalEntry]]
      case "String"    => obj.toString.asJson.convertTo[DataSet[StringEntry]]
      case "Boolean"   => obj.toString.asJson.convertTo[DataSet[BooleanEntry]]
      case "Summed"    => obj.toString.asJson.convertTo[DataSet[SummedEntry]]
      case "NumericalStreamChunk" => obj.toString.asJson.convertTo[DataSet[NumericalStreamChunkEntry]]
    }
    SenMLBuilder.build(r)
  }
  
  def getSchema(sensor: String): String = {
    require(exists(sensor), "Unknown sensor")
    val obj = collection.findOne(MongoDBObject("s" -> sensor)).get
    obj.getAs[String]("k").get
  }
  
  def push(sensor: String, senml: SenML): List[MeasurementOrParameter] = {
    require(exists(sensor), "Unknown sensor")
    val dataset = senml.canonized
    dataset.measurementsOrParameters match {
      case None => List()
      case Some(lst) => {
	    val (accepted, rejected) = lst partition { mop => mop.name == sensor }
	    val set = collection.findOne(MongoDBObject("s" -> sensor)).get
	    val baseTime = set.getAs[Long]("bt").get
	    val dataSet = set.get("e").asInstanceOf[BasicDBList]
	    accepted foreach { mop => dataSet += data2json(baseTime, mop) }
	    rejected
      }
    }
  }
  
  private[this] def collection = {
    val mongoConn = MongoConnection()
    mongoConn("sensapp_db")("raw_data")
  }
  
  implicit def json2dbObj(json: String): DBObject = {
    val raw = JSON.parse(json)
    if (null == raw)
      throw new RuntimeException("Unable to parse JSON data") 
    raw.asInstanceOf[BasicDBObject].asDBObject
  }  
}