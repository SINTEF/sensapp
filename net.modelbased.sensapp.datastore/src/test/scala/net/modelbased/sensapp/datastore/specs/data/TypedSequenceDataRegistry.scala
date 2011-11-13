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
 * This registry handles persistent object made by collections of complex data
 * @author Sebastien Mosser
 */
class TypedSequenceDataRegistry extends DataModelRegistry[TypedSequenceData]{

  override val collectionName = "sequence_data"
  
  override def serialize(obj: TypedSequenceData): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += ("n" -> obj.n)
    builder += ("v" -> obj.v.map {e => MongoDBObject("n" -> e.n, "v" -> e.v)})
    builder.result
  }
  
  override def deserialize(dbObj: DBObject): TypedSequenceData = {
    val caster: (AnyRef => MultiTypedData) = { any =>
      val dbObj = any.asInstanceOf[DBObject]
      MultiTypedData(dbObj.as[String]("n"), dbObj.as[Int]("v"))
    }
    val l = extractListAs[MultiTypedData](dbObj, "v", caster)
    TypedSequenceData(dbObj.as[String]("n"), l)
  }
}