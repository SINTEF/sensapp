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
 * Module: net.modelbased.sensapp.library.datastore
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
package net.modelbased.sensapp.library.datastore

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.MongoDBObjectBuilder
import com.mongodb.util.JSON

/**
 * This trait defines manipulations used to manipulate DataStore Elements
 * 
 * @author Sebastien Mosser
 * 
 * @param T the type of the elements stored in the DataStore
 */
trait DataStoreOperations[T] extends DataSpecific[T] {
  
  /**
   * the exists method check if an object exists in the datastore based on a criterion
   * 
   * Assumption: the given criterion identify only ONE object, or none. 
   * 
   * @param id the criterion used to identify the object
   *  @return False if no match found, True if 'id' matched an object.
   */
  def exists(id: Criterion): Boolean = {
    pull(id) match {
      case Some(_) => true
      case None => false
    }
  }
  
  /**
   * The pull method retrieve an object from the DataStore based on a criterion.
   * 
   * Assumption: the given criterion identify only ONE object, or none. 
   * 
   * @param id the criterion used to identify the object
   * @return None if no match found, Some(obj) if 'obj' matched.
   */
  def pull(id: Criterion): Option[T] = {
    val dbResult = _collection.findOne(MongoDBObject(id._1 -> id._2))
    dbResult match {
      case Some(dbObj) => Some(toDomainObject(dbObj)) 
      case None => None
    }
  }
  
  /**
   * The push method store an object in the DataStore. 
   * 
   * It will erase another object that return the same identifier criteria 
   * (see the identify method in the DataSpecific trait).
   * 
   * @param obj the object to be stored
   * 
   */
  def push(obj: T) {
    pull(identify(obj)) match {
      case Some(db) => drop(db)
      case _ => 
    }
    _collection += toDatabaseObject(obj)
  }
  
  /**
   * The retrieve method is used to perform a criteria-based query
   * 
   * @param criteria a list of expected criterion (AND semantics)
   * @return a list of objects that EXACTLY matched these criteria
   */
  def retrieve(criteria: List[(String, Any)]): List[T] = {
    val prototype = MongoDBObject.newBuilder
    criteria foreach { c => prototype += (c._1 -> c._2) }
    _collection.find(prototype.result).toList map { toDomainObject(_) }
  }
  
  /**
   * the drop method remove an object from the registry.
   * 
   * The removal is serialization-based. It will remove from the registry
   * an element that is serialized exactly as its given parameter.
   * 
   * @param obj the T instance to be dropped out
   */
  def drop(obj: T) { _collection -= toDatabaseObject(obj) }
  
  /**
   * The dropAll method clear the content of the registry.
   * 
   * Use with caution.
   */
  def dropAll() { _collection.drop() }
  
  /**
   * Compute the size of the registry, that is, the number of object currently 
   * stored.
   * 
   * @return the size of the registry
   */
  def size = _collection.size  
  
  /**
   * The underlying MongoDB collection
   */
  @MongoDBSpecific
  protected lazy val _collection = {
    val conn = MongoConnection()
    val db = conn(databaseName)
    val col = db(collectionName)
    col.ensureIndex(key)
    col
  }
  
  /**
   * Transform a domain object (T)  into an instance of DBObject (mongoDB specific)
   * 
   * @param obj the object to be transformed
   * @return the associated DBObject
   */
  @MongoDBSpecific
  private def toDatabaseObject(obj: T): DBObject = {
    val raw = JSON.parse(serialize(obj))
    if (null == raw)
      throw new RuntimeException("Unable to parse JSON data") // FIXME (DataStoreException)
    raw.asInstanceOf[BasicDBObject].asDBObject
  }
  
  /**
   * Transform a DBObject (MongoDB) into an instance of the domain object (T)
   * 
   * @param dbobj the DBObject to be transformed
   * @return the associated domain object (T)
   */
  @MongoDBSpecific
  private def toDomainObject(dbObj: DBObject): T = { deserialize(dbObj.toString) }
  
}