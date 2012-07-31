package net.modelbased.sensapp.service.rrd.data

/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.service.rrd
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
import cc.spray.json._
import net.modelbased.sensapp.library.datastore._
import net.modelbased.sensapp.service.rrd.data.RRDJsonProtocol._
import io.BytePickle.Def

/**
 * Persistence layer associated to the RRDTemplate class
 * 
 * @author Sebastien Mosser
 */
class RRDGraphTemplateRegistry extends DataStore[RRDGraphTemplate]  {

  override val databaseName = "sensapp_db"
  override val collectionName = "rrd.graphtemplates"
  override val key = "key"

  def populateDB() = {
     // getClass.getResource("/resources/rrd_templates").g
     //println(">>>>>>>>>>>> populateDB")
    IOUtils.getResourceListing(getClass, "rrd_graphtemplates/").foreach{ name : String =>
      //println(">>>>>>>>>>>> pushing template " + name)
      var instream = getClass.getResourceAsStream("/rrd_graphtemplates/" + name)
      var xml = IOUtils.readStream(instream)
      var template = new RRDGraphTemplate(name, xml)
      push(template)
    }
  }

  override def getIdentifier(e: RRDGraphTemplate) = {
     e.key
  }
  
  override def deserialize(json: String): RRDGraphTemplate = { json.asJson.convertTo[RRDGraphTemplate] }
 
  override def serialize(e: RRDGraphTemplate): String = { e.toJson.toString }

}
