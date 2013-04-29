/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 * Module: net.modelbased.sensapp
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
package net.modelbased.sensapp.service.converter.request

import cc.spray.json._
import net.modelbased.sensapp.library.datastore._
import net.modelbased.sensapp.service.converter.request.CSVDescriptorProtocols.csvDescriptorFormat

/**
 * Persistence layer associated to the CSVDescriptor class
 * 
 * @author Brice Morin
 */
class CSVDescriptorRegistry extends DataStore[CSVDescriptor]  {

  override val databaseName = "sensapp_db"
  override val collectionName = "csv_desc" 
  override val key = "name"
    
  override def getIdentifier(desc: CSVDescriptor) = desc.name
  
  override def deserialize(json: String): CSVDescriptor = { json.asJson.convertTo[CSVDescriptor] }
 
  override def serialize(desc: CSVDescriptor): String = { desc.toJson.toString }
    
}