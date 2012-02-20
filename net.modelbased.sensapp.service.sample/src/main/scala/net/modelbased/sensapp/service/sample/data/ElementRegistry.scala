/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.service.sample
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
package net.modelbased.sensapp.service.sample.data

import cc.spray.json._
import net.modelbased.sensapp.library.datastore._
import net.modelbased.sensapp.service.sample.data.ElementJsonProtocol.format

/**
 * Persistence layer associated to the Element class
 * 
 * @author Sebastien Mosser
 */
class ElementRegistry extends DataStore[Element]  {

  override val databaseName = "sensapp_db"
  override val collectionName = "sample.elements" 
    
  override def identify(e: Element) = ("key", e.key)
  
  override def deserialize(json: String): Element = { json.asJson.convertTo[Element] }
 
  override def serialize(e: Element): String = { e.toJson.toString }
    
}