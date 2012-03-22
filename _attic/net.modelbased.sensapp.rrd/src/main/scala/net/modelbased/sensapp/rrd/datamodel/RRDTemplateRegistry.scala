/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.rrd
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
package net.modelbased.sensapp.rrd.datamodel

/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.registry
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
import net.modelbased.sensapp.datastore._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.MongoDBObjectBuilder
import com.mongodb.util.JSON
class RRDTemplateRegistry extends DataStore[RRDTemplate] {
  
  override val databaseName = "sensapp_db"
  override val collectionName = "rrdtemplates"
    
  override def identify(t: RRDTemplate) = ("id", t.id)
  
  override def deserialize(dbObj: DBObject): RRDTemplate = {
    val id: String = dbObj.as[String]("id")
    val template: String = dbObj.as[String]("template")
    new RRDTemplate(id, template)
  }
 
  override def serialize(obj: RRDTemplate): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += ("id" -> obj.id)
    builder += ("template" -> obj.template)
    builder.result
  }
}