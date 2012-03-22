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
package net.modelbased.sensapp.rrd.datastore

import net.modelbased.sensapp.datastore.MongoDBSpecific
import com.mongodb.casbah.MongoConnection
import org.rrd4j.core.RrdDef

/**
 * Created by IntelliJ IDEA.
 * User: ffl
 * Date: 15.11.11
 * Time: 15:09
 * To change this template use File | Settings | File Templates.
 */

object RRDMongoDBDataStore {

  val databaseName = "sensapp_db"
  val collectionName = "rrddata"

  def createDB(id : String, rrdDef: RrdDef) {

  }


  /**
   * The underlying MongoDB collection
   */
  @MongoDBSpecific
  private  lazy val _collection = {
    val conn = MongoConnection()
    val db = conn(databaseName)
    val col = db(collectionName)
    col
  }
}