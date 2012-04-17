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
import java.util.jar.{JarEntry, JarFile}
import java.io._
import java.lang.StringBuilder
import net.modelbased.sensapp.service.rrd.data.RRDBaseJsonProtocol.format
import com.mongodb.{DBAddress, DBCollection, DB, Mongo}
import org.specs2.internal.scalaz.Validation
import java.net.{URLConnection, URLDecoder, URL}
import org.rrd4j.core.{RrdDefTemplate, RrdMongoDBBackendFactory, RrdDb}

/**
 * Persistence layer associated to the RRDBase class
 * 
 * @author Sebastien Mosser
 */
class RRDBaseRegistry extends DataStore[RRDBase]  {

  override val databaseName = "sensapp_db"
  override val collectionName = "rrd.bases"

  val rrd4jDatabaseName = "sensapp_db"
  val rrd4jCollectionName = "rrd.data"

  val rrd4jcollection = new Mongo( new DBAddress("localhost", "27017" ) ).getDB(rrd4jDatabaseName).getCollection(rrd4jCollectionName)
  val rrd4jfactory = new RrdMongoDBBackendFactory(rrd4jcollection);

  def createRRD4JBase(b: RRDBase) = {
      // TODO: catch the numerous exceptions which could be raised here
      val xml = sendGetRequest(b.template_location, null);
      if (xml != null) {
        val template = new RrdDefTemplate(xml)
        template.setVariable("PATH", b.path);
        val db = new RrdDb(template.getRrdDef, rrd4jfactory)
        db.close
      }
  }

  def getRRD4JBase(b: RRDBase) : RrdDb = {
    val result = new RrdDb(b.path, rrd4jfactory)
    return result
  }

  /*
  def populateDB() = {
     // getClass.getResource("/resources/rrd_templates").g
     println(">>>>>>>>>>>> populateDB")
    getResourceListing(getClass, "rrd_templates/").foreach{ name : String =>
      println(">>>>>>>>>>>> pushing template " + name)
      var instream = getClass.getResourceAsStream("/rrd_templates/" + name)
      var xml = readStream(instream)
      var template = new RRDTemplate(name, xml)
      push(template)
    }
  }
    */

    
  override def identify(e: RRDBase) = ("path", e.path)
  
  override def deserialize(json: String): RRDBase = { json.asJson.convertTo[RRDBase] }
 
  override def serialize(e: RRDBase): String = { e.toJson.toString }

  def sendGetRequest(endpoint: String, requestParameters: String): String = {
    var result: String = null
    if (endpoint.startsWith("http://")) {
      try {
        var data: StringBuffer = new StringBuffer
        var urlStr: String = endpoint
        if (requestParameters != null && requestParameters.length > 0) {
          urlStr += "?" + requestParameters
        }
        var url: URL = new URL(urlStr)
        var conn: URLConnection = url.openConnection
        var rd: BufferedReader = new BufferedReader(new InputStreamReader(conn.getInputStream))
        var sb: StringBuffer = new StringBuffer
        var line: String = null
        while ((({
          line = rd.readLine; line
        })) != null) {
          sb.append(line)
        }
        rd.close
        result = sb.toString
      }
      catch {
        case e: Exception => {
          e.printStackTrace
        }
      }
    }
    return result
  }

}
