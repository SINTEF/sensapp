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
//import org.specs2.internal.scalaz.Validation
import java.net.{URLConnection, URLDecoder, URL}
import org.rrd4j.core.{RrdDefTemplate, RrdMongoDBBackendFactory, RrdDb}
import org.xml.sax.XMLReader
import com.mongodb._
import org.parboiled.support.Var
import java.awt.Cursor

import com.mongodb.casbah.Imports._
import java.util.ArrayList

import scala.collection.JavaConversions._

/**
 * Persistence layer associated to the RRDBase class
 * 
 * @author Sebastien Mosser
 */
class RRDBaseRegistry {

  val rrd4jDatabaseName = "sensapp_db"
  val rrd4jCollectionName = "rrd.databases"

  // TODO: Use the default Sensapp DB here
  val rrd4jcollection = new Mongo( new com.mongodb.DBAddress("localhost", "27017" ) ).getDB(rrd4jDatabaseName).getCollection(rrd4jCollectionName)
  val rrd4jfactory = new RrdMongoDBBackendFactory(rrd4jcollection);

  def listRRD4JBases() : ArrayList[String] = {
    // Had to query the DB . No method in the RRD4J APIs.
    val result = new ArrayList[String]()
    val q  = MongoDBObject.empty
    val fields = MongoDBObject("path" -> 1)
    val res = rrd4jcollection.find(q, fields)

    res.toArray.foreach{o =>
        result.add(o.get("path").toString)
    }

    return result
  }

  def deleteRRD4JBase(path : String) = {
    // Had to query the DB . No method in the RRD4J APIs.
    val query = MongoDBObject("path" -> path)
    val rrdObject = rrd4jcollection.findOne(query);
    if (rrdObject != null) {
        rrd4jcollection.remove(rrdObject)
    }
  }

  def createRRD4JBase(path : String, template_url : String) = {
      // TODO: catch the numerous exceptions which could be raised here
      val xml = sendGetRequest(template_url, null);
      if (xml != null) {
        val template = new RrdDefTemplate(xml)
        template.setVariable("PATH", path);
        val rrddef = template.getRrdDef
        rrddef.setPath(path)
        val db = new RrdDb(rrddef, rrd4jfactory)
        db.close
      }
  }

  def importRRD4JBase(path : String, data_url : String) = {
      // TODO: catch the numerous exceptions which could be raised here
      val xmlfile = downloadTmpFile(data_url, null)
      if (xmlfile != null) {
        val db = new RrdDb(path, xmlfile.getAbsolutePath, rrd4jfactory)
        db.close
        xmlfile.delete
      }
  }

  def getRRD4JBase(path : String, ro:Boolean) : RrdDb = {
    val result = new RrdDb(path, ro, rrd4jfactory)
    return result
  }

  /*
  def populateDB() = {

  }
    */

  def sendGetRequest(endpoint: String, requestParameters: String): String = {
    var result: String = null
    if (endpoint.startsWith("http://")) {
      try {
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

    def downloadTmpFile(endpoint: String, requestParameters: String): File = {

      var result: File = File.createTempFile("sensapp_", "xml")
      result.deleteOnExit

      if (endpoint.startsWith("http://")) {
      try {
        var bw = new BufferedWriter(new FileWriter(result));
        var urlStr: String = endpoint
        if (requestParameters != null && requestParameters.length > 0) {
          urlStr += "?" + requestParameters
        }
        var url: URL = new URL(urlStr)
        var conn: URLConnection = url.openConnection
        var rd: BufferedReader = new BufferedReader(new InputStreamReader(conn.getInputStream))
        var line: String = null
        while ((({
          line = rd.readLine; line
        })) != null) {
          bw.append(line)
        }
        rd.close
        bw.close()
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
