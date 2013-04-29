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
package net.modelbased.sensapp.registry.datamodel

import net.modelbased.sensapp.datastore._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.MongoDBObjectBuilder
import com.mongodb.util.JSON
//import scala.collection.mutable

class SensorRegistry extends DataStore[Sensor] {
  
  override val databaseName = "sensapp_db"
  override val collectionName = "sensors"
    
  override def identify(sensor: Sensor) = ("id", sensor.id)
  
  override def deserialize(dbObj: DBObject): Sensor = {
    val id: String = dbObj.as[String]("id")
    val nickname: Option[String] = dbObj.getAs[String]("nickname")
    new Sensor(id, nickname)
  }
 
  override def serialize(obj: Sensor): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += ("id" -> obj.id)
    extract(obj.nickname, "nickname", builder)
    builder.result
  }
}
