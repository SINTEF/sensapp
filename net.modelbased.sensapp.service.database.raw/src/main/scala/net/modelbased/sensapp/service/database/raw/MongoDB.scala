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
import net.modelbased.sensapp.library.senml.MeasurementOrParameter
import com.mongodb.casbah.Imports._
import com.mongodb.util.JSON
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.commons.MongoDBList

class MongoDB extends Backend {

  def content: List[String] = {
    val data = collection.find(MongoDBObject.empty, MongoDBObject("sensor" -> 1))
    val it = data map {e => e.getAs[String]("s").get }
    it.toList
  }
  
  def exists(sensor: String): Boolean = {
    val data = collection.findOne(MongoDBObject("s" -> sensor), MongoDBObject("sensor" -> 1))
    data != None
  }
  
  def create(request: CreationRequest): Boolean = {
    collection += request2json(request)
    true
  }
  
  def describe(sensor: String): Option[SensorDatabaseDescriptor] = {
    collection.findOne(MongoDBObject("s" -> sensor)) match {
      case None => None
      case Some(dbObj) => Some(SensorDatabaseDescriptor(dbObj.getAs[String]("s").get))
    }
  }
  
  def delete(sensor: String): Boolean = {
    collection.findOne(MongoDBObject("s" -> sensor)) match {
      case None => false
      case Some(dbObj) => { collection -= dbObj; true }
    }
  }
 
  def push(sensor: String, dataset: List[MeasurementOrParameter]): List[MeasurementOrParameter] = {
    require(exists(sensor), "Unknown sensor")
    val (accepted, rejected) = dataset partition { mop => mop.name == sensor }
    val set = collection.findOne(MongoDBObject("s" -> sensor)).get
    val baseTime = set.getAs[Long]("bt").get
    val dataSet = set.get("e").asInstanceOf[MongoDBList]
    accepted foreach { mop => dataSet += data2json(baseTime, mop) }
    rejected
  }
  
  private[this] def collection = {
    val mongoConn = MongoConnection()
    mongoConn("sensapp")("raw_data")
  }
  
  implicit def json2dbObj(json: String): DBObject = {
    val raw = JSON.parse(json)
    if (null == raw)
      throw new RuntimeException("Unable to parse JSON data") 
    raw.asInstanceOf[BasicDBObject].asDBObject
  }  
}