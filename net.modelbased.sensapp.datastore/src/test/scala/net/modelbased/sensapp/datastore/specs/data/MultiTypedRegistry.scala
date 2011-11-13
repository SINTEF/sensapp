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
package net.modelbased.sensapp.datastore.specs.data

import com.mongodb.casbah.Imports._

/**
 * This Registry handle very simple persistent objects.
 * @author Sebastien Mosser
 */
class MultiTypedRegistry extends DataModelRegistry[MultiTypedData] {
  
  override val collectionName = "multi_typed"
  
  override def serialize(obj: MultiTypedData): DBObject = {
    MongoDBObject("n" -> obj.n, "v" -> obj.v)
  }
  
  override def deserialize(dbObj: DBObject): MultiTypedData = {
    MultiTypedData(dbObj.as[String]("n"), dbObj.as[Int]("v"))
  }
}