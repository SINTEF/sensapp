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

import net.modelbased.sensapp.library.datastore.DataStore
import com.mongodb.casbah.Imports._
 
/**
 * DataModel is the root of the test data model class hierarchy
 * 
 * <strong>Remark</strong>: here, a data will always be identified by its "n" value.
 * @author Sebastien Mosser
 */
sealed abstract class DataModel(val n: String)

/**
 * MultiTypedData illustrates a persistent object made with a String and an Int.
 * @author Sebastien Mosser
 */
case class MultiTypedData(x: String, v: Long) extends DataModel(x)

/**
 * SequenceData illustrates how persistent collections of scalar data are handled
 * @author Sebastien Mosser
 */
case class SequenceData(x: String, v: List[Long]) extends DataModel(x)

/**
 * TypedSequenceData illustrates how persistent collections of complex objects are handled
 * @author Sebastien Mosser
 */
case class TypedSequenceData(x: String, v: List[MultiTypedData]) extends DataModel(x)


/**
 * An abstract DataModel registry.
 * 
 * It groups the database name and the identifier function at the sane place.
 * @author Sebastien Mosser
 */
abstract class DataModelRegistry[T <: DataModel] extends DataStore[T] {
  //FIXME: this arch. does not allow to restrict a DataModelRegistry to handle only ONE type
  override final val databaseName = "sensapp_datastore_test"  
  override def getIdentifier(data: T) = data.n
  
  //override def identify(data: T) = ("n", data.n)
  
  override val key = "n" 
}



