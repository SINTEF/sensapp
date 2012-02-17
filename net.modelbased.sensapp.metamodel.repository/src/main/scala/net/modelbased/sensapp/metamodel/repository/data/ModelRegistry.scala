/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.metamodel.repository
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
package net.modelbased.sensapp.metamodel.repository.data

import net.modelbased.sensapp.datastore._
import com.mongodb.casbah.Imports._
import cc.spray.json._

import net.modelbased.sensapp.metamodel.repository.data.ModelJsonProtocol.modelFormat
/**
 * Persistence layer associated to the Model class
 * 
 * @author Sebastien Mosser
 */
class ModelRegistry extends DataStore[Model]  {

  override val databaseName = "sensapp_db"
  override val collectionName = "models.registry" 
    
  override def identify(m: Model) = ("name", m.name)
  
  override def deserialize(json: String): Model = { json.asJson.convertTo[Model] }
 
  override def serialize(obj: Model): String = { obj.toJson.toString }
    
}