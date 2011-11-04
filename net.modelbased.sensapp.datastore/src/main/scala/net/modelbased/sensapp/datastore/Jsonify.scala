/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.datastore
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
package net.modelbased.sensapp.datastore

import com.mongodb.util.JSON
import com.mongodb.casbah.Imports._

/**
 * This trait provides fron and to JSON serialization mechanism
 * 
 * <strong>Remark</strong>: If the DataSpecific trait is enhanced to deal with 
 * pure JSON representation (as it should be), this trait will be useless.
 */
trait Jsonify[T] extends DataSpecific[T] {
  
  /**
   * Deserialize a JSON string into an instance of T
   * 
   * @param json the string to be considered
   * @return the associated instance of T
   */
  @MongoDBSpecific
  def fromJSON(json: String): T = {
    val dbObj = JSON.parse(json).asInstanceOf[BasicDBObject].asDBObject
    deserialize(dbObj)
  }
  
  /**
   * Serialize an instance of T as a JSON string
   * 
   * @param obj the object to be jsonified
   * @return its associated JSON representation
   */
  def toJSON(obj: T): String = { serialize(obj).toString }
}