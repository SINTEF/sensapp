package net.modelbased.sensapp.service.rrd.data

import java.io._
import java.net.{URLDecoder, URLConnection, URL}
import java.util.jar.{JarEntry, JarFile}

/**
 * Created by IntelliJ IDEA.
 * User: franck
 * Date: 19/04/12
 * Time: 21:53
 * To change this template use File | Settings | File Templates.
 */

object IOUtils {


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

  def readStream(stream: InputStream): String = {
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