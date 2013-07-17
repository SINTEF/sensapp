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
 * Module: net.modelbased.sensapp.service.registry
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
package net.modelbased.sensapp.service.registry.data

import cc.spray.json._
import net.modelbased.sensapp.library.datastore._
import ElementJsonProtocol._

/**
 * Persistence layer associated to the Element class
 * 
 * @author Sebastien Mosser
 */
class SensorDescriptionRegistry extends DataStore[SensorDescription]  {

  override val databaseName = "sensapp_db"
  override val collectionName = "registry.sensors" 
  override val key = "id"
    
  override def getIdentifier(e: SensorDescription) = e.id
  
  override def deserialize(json: String): SensorDescription = { json.asJson.convertTo[SensorDescription] }
 
  override def serialize(e: SensorDescription): String = { e.toJson.toString }
    
}

class CompositeSensorDescriptionRegistry extends DataStore[CompositeSensorDescription]  {

  override val databaseName = "sensapp_db"
  override val collectionName = "registry.sensors.composite" 
    
  override val key = "id"
    
  override def getIdentifier(e: CompositeSensorDescription) = e.id
  
  override def deserialize(json: String): CompositeSensorDescription = { json.asJson.convertTo[CompositeSensorDescription] }
 
  override def serialize(e: CompositeSensorDescription): String = { e.toJson.toString }
    
}