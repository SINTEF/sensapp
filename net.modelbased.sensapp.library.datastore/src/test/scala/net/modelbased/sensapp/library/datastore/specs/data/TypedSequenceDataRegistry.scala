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
package net.modelbased.sensapp.library.datastore.specs.data

import com.mongodb.casbah.Imports._
import com.mongodb.util.JSON

/**
 * This registry handles persistent object made by collections of complex data
 * @author Sebastien Mosser
 */
class TypedSequenceDataRegistry extends DataModelRegistry[TypedSequenceData]{

  override val collectionName = "sequence_data"
  
  override def serialize(obj: TypedSequenceData): String = {
    val builder = MongoDBObject.newBuilder
    builder += ("n" -> obj.n)
    builder += ("v" -> obj.v.map {e => MongoDBObject("n" -> e.n, "v" -> e.v)})
    builder.result.toString
  }
  
  override def deserialize(json: String): TypedSequenceData = {
    val dbObj = JSON.parse(json).asInstanceOf[BasicDBObject].asDBObject
    val caster: (AnyRef => MultiTypedData) = { any =>
      val dbObj = any.asInstanceOf[DBObject]
      MultiTypedData(dbObj.as[String]("n"), dbObj.as[Long]("v"))
    }
    val l = extractListAs[MultiTypedData](dbObj, "v", caster)
    TypedSequenceData(dbObj.as[String]("n"), l)
  }
}