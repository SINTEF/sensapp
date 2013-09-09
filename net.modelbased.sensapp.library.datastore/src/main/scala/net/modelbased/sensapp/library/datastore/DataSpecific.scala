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

/**
 * This trait groups all the operation specific to a given data type in 
 * a registry. These operations need to be user-provided. They are used 
 * by the framework to implement all the datastore logic.
 * 
 * <strong>Drawback</strong>: This implementation is too strongly coupled
 * to MongoDB. It should not be the case in an ideal world (e.g., pure JSON 
 * should be used instead of DBObject).
 * 
 * @param T the type of the data to be stored in the registry
 * @author Sebastien Mosser
 */
abstract trait DataSpecific[T] {
  
  /**
   * The name of the database to be used to store the registry
   */
  @MongoDBSpecific
  protected val databaseName: String
  
  /**
   * The name of the collection to be used to store the objects
   */
  @MongoDBSpecific
  protected val collectionName: String
  
  /**
   * The "colum" name to be used in the database to index the content
   */
  protected val key: String
  
  /**
   * This operation deserialize an object retrieved from the DB into an 
   * instance of T
   * 
   * @param dbObj A document retrieved from the MongoDB collection
   * @return the associated instance of T
   */
  @MongoDBSpecific
  protected def deserialize(json: String): T
  
  /**
   * This operation serialize an instance of T into a DB Object
   * 
   * @param obj the object to be serialized
   * @return the associated DB Object
   */
  @MongoDBSpecific
  protected def serialize(obj: T): String
  
  /**
   * A Criterion is a (key: String, value: Any) tuple used to identify object
   * in the database
   */
  protected type Criterion = (String, Any)
  
  /**
   * Compute a value that, when combined with "this.key", builds a Criteria used 
   * to UNIQUELY IDENTIFY an object.
   */
  protected def getIdentifier(obj: T): Any
  
  /**
   * Compute a criteria used to UNIQUELY IDENTIFY an object. The DataStore
   * framework STRONGLY assumes that two different objects will return two
   *  different criteria through this method.
   *  
   *  @param obj the instance of T to be identified
   *  @return a UNIQUE criteria used to identify this object
   */
   final def identify(obj: T): Criterion = (this.key, getIdentifier(obj))
}