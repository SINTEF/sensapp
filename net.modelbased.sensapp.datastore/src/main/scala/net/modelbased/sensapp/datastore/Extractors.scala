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

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.MongoDBObjectBuilder

/**
 * This trait implements useful operations to extract data from MongoDB object
 * 
 * @author Sebastien Mosser
 */
trait Extractors[T] extends DataSpecific[T] {
  
  /**
   * Extract[C] is used to add to a Mongo builder the content of a data, if 
   * such a content exists. In the other case, it will just do nothing.
   * 
   * @param data An option[C] to be added in a DBObject
   * @param field the name of the field to use in the MongoDBObject
   * @param builder the builder that will contains the extracted data, if any.
   */
  @MongoDBSpecific
   protected def extract[C](data: Option[C], field: String, builder: MongoDBObjectBuilder) {
    data match {
      case Some(d) => builder += ( field -> d.toString )
      case _ =>
    }
  }
  
   /**
    * Extract in a given DBObject a list of DBObject stored in a given field
    * 
    * @param obj the DBObject used as extraction basis 
    * @param name the field name
    * @return the list of retrieved objects
    */
  @MongoDBSpecific
  protected def extractList(obj: DBObject, name: String): List[DBObject] = {
    val tmp = obj.as[BasicDBList](name)
    tmp.toList map { _.asInstanceOf[DBObject] }
  }
}