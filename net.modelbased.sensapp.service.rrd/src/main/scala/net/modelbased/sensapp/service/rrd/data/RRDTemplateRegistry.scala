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
import java.net.{URLDecoder, URL}
import java.util.jar.{JarEntry, JarFile}
import java.io._
import org.parboiled.support.Var
import javax.swing.text.AbstractDocument.Content
import java.lang.StringBuilder

/**
 * Persistence layer associated to the RRDTemplate class
 * 
 * @author Sebastien Mosser
 */
class RRDTemplateRegistry extends DataStore[RRDTemplate]  {

  override val databaseName = "sensapp_db"
  override val collectionName = "rrd.templates"

  def populateDB() = {
     // getClass.getResource("/resources/rrd_templates").g
     //println(">>>>>>>>>>>> populateDB")
    getResourceListing(getClass, "rrd_templates/").foreach{ name : String =>
      //println(">>>>>>>>>>>> pushing template " + name)
      var instream = getClass.getResourceAsStream("/rrd_templates/" + name)
      var xml = readStream(instream)
      var template = new RRDTemplate(name, xml)
      push(template)
    }
  }

    
  override def identify(e: RRDTemplate) = ("key", e.key)
  
  override def deserialize(json: String): RRDTemplate = { json.asJson.convertTo[RRDTemplate] }
 
  override def serialize(e: RRDTemplate): String = { e.toJson.toString }

  /**
   * List directory contents for a resource folder. Not recursive.
   * This is basically a brute-force implementation.
   * Works for regular files and also JARs.
   *
   * @author Greg Briggs
   * @param clazz Any java class that lives in the same place as the resources you want.
   * @param path Should end with "/", but not start with one.
   * @return Just the name of each member item, not the full paths.
   * @throws URISyntaxException
   * @throws IOException
   */
  def getResourceListing(clazz: Class[_], path: String): Array[String] = {
    var dirURL: URL = clazz.getClassLoader.getResource(path)
    if (dirURL != null && (dirURL.getProtocol == "file")) {
      //println(">>>>>>>>>>>> dirURL != null && (dirURL.getProtocol == \"file\")")
      return new File(dirURL.toURI).list
    }
    if (dirURL == null) {
      //println(">>>>>>>>>>>> dirURL == null")
      var me: String = clazz.getName.replace(".", "/") + ".class"
      dirURL = clazz.getClassLoader.getResource(me)
    }
    if (dirURL.getProtocol == "jar") {
      //println(">>>>>>>>>>>> dirURL.getProtocol == \"jar\"")
      var jarPath: String = dirURL.getPath.substring(5, dirURL.getPath.indexOf("!"))
      var jar: JarFile = new JarFile(URLDecoder.decode(jarPath, "UTF-8"))
      var entries: java.util.Enumeration[JarEntry] = jar.entries
      var result: java.util.Set[String] = new java.util.HashSet[String]
      while (entries.hasMoreElements) {
        var name: String = entries.nextElement.getName
        //println(">>>>>>>>>>> name = " + name)
        if (name.startsWith(path)) {
          var entry: String = name.substring(path.length)
          var checkSubdir: Int = entry.indexOf("/")
          if (checkSubdir >= 0) {
            entry = entry.substring(0, checkSubdir).trim
          }
           //println(">>>>>>>>>>> ADD = " + entry)
          if (entry.length() > 0) result.add(entry)
        }
      }
      return result.toArray(new Array[String](result.size))
    }
    throw new UnsupportedOperationException("Cannot list files for URL " + dirURL)
  }

  private def readStream(stream: InputStream): String = {
    var f: BufferedReader = null
    val result = new StringBuilder
    try {
      f = new BufferedReader(new InputStreamReader(stream))
      while( f.ready() ) {
        result.append(f.readLine())
        result.append('\n')
      }
    }
    finally {
      if (f != null) try {
        f.close
      }
      catch {
        case ignored: IOException => {
        }
      }
    }
    return result.toString
  }

}
