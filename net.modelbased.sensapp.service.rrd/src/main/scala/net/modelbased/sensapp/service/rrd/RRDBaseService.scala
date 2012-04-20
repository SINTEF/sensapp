package net.modelbased.sensapp.service.rrd

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
import cc.spray._
import cc.spray.http._
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import cc.spray.directives._
import xml.XML

import scala.collection.JavaConversions._

import net.modelbased.sensapp.service.rrd.data._
import net.modelbased.sensapp.service.rrd.data.RRDJsonProtocol._
import org.rrd4j.core.Util
import java.text.SimpleDateFormat

import net.modelbased.sensapp.library.senml._

// Application specific:
import net.modelbased.sensapp.service.rrd.data._
import net.modelbased.sensapp.library.system.{Service => SensAppService}

trait RRDBaseService extends SensAppService {

  private[this] val _registry = new RRDBaseRegistry()

  val service = {
    path("rrd" / "databases") {
      get { context =>
        val uris = _registry.listRRD4JBases().toArray().map { s => context.request.path  + "/" + s }
        context complete uris
      } ~
      detach {
        post {
          content(as[RRDCreateAndImport]) { element => context =>
            if (_registry.listRRD4JBases().contains(element.path)){
              context fail (StatusCodes.Conflict, "A RRD database identified as ["+ element.path +"] already exists!")
            } else {
              _registry.importRRD4JBase(element.path, element.data_url)
              context complete (StatusCodes.Created, buildUrl(context, element.path))
            }
          } ~
          content(as[RRDCreateFromTemplate]) { element => context =>
            if (_registry.listRRD4JBases().contains(element.path)){
              context fail (StatusCodes.Conflict, "A RRD database identified as ["+ element.path +"] already exists!")
            } else {
              _registry.createRRD4JBase(element.path, element.template_url)
              context complete (StatusCodes.Created, buildUrl(context, element.path))
            }
          }
        }
      }
    } ~
    path("rrd" / "databases" / "[^/]+".r) { path =>
      get { context =>
          val db = _registry.getRRD4JBase(path, true)
          if (db != null) {
            /*
            val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            var result =  dateFormat.format(Util.getCalendar(db.getLastArchiveUpdateTime).getTime) + " -> [ "
            db.getLastDatasourceValues.foreach{ d =>
              result += d.toString + " "
            }
            result += "]"
            */
             context complete (StatusCodes.OK, _registry.getgetRRD4JBaseDescription(db))
          }
          else {
             context fail (StatusCodes.Conflict, "RRD databbase identified as ["+ path +"] was not found!")
          }
      } ~
      delete { context =>
        _registry.deleteRRD4JBase(path)
           context complete (StatusCodes.OK, buildUrl(context, path))
      } /*~
      put {
         handle data
        }
      }*/
    } ~
    path("rrd" / "databases" / "[^/]+".r / "template") { path =>
      get { context =>
        val db = _registry.getRRD4JBase(path, true)
        if (db != null) {
           context complete(XML.loadString(db.getRrdDef.exportXmlTemplate()))

        }
        else {
           context fail (StatusCodes.Conflict, "RRD databbase identified as ["+ path +"] was not found!")
        }
      }
    } ~
    path("rrd" / "databases" / "[^/]+".r / "xml") { path =>
      detach {
        get { context =>
          val db = _registry.getRRD4JBase(path, true)
            if (db != null) {
               context complete(XML.loadString(db.getXml))
            }
            else {
               context fail (StatusCodes.Conflict, "RRD databbase identified as ["+ path +"] was not found!")
            }
        }
      }
    } ~
    path("rrd" / "databases" / "[^/]+".r / "data") { path =>
      get { parameters("start" ? "now", 'end ? "now", 'resolution ? "3600", 'funtion ? "AVERAGE") { (start, end, resolution, func) =>
            val db = _registry.getRRD4JBase(path, true)
            if (db != null) {
              val query = RRDRequest(func, start, end, resolution)
              println(">>>>>>>>>>>>>>query " + query.getFunction + " " + query.getStart +" "+ query.getEnd + " " + query.getResolution)
              val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              val fr = db.createFetchRequest(query.getFunction, query.getStart, query.getEnd, query.getResolution)
              val data = fr.fetchData()
              val out = new StringBuilder()
              val ts = data.getTimestamps
              val vs = data.getValues
              for (i <- 0 until data.getRowCount) {
                out append dateFormat.format(Util.getDate(ts(i)))
                vs.foreach{ v =>
                  out append "\t"
                  out append v(i).toString
                }
                out append "\n"
              }
              _.complete(out.toString)
            }
            else {
               _.fail(StatusCodes.Conflict, "RRD databbase identified as ["+ path +"] was not found!")
            }
        }
/*


        val db = _registry.getRRD4JBase(path, true)
          if (db != null) {

            val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            var result =  dateFormat.format(Util.getCalendar(db.getLastArchiveUpdateTime).getTime) + " -> [ "
            db.getLastDatasourceValues.foreach{ d =>
              result += d.toString + " "
            }
            result += "]"

             context complete (StatusCodes.OK, result)
          }
          else {
             context fail (StatusCodes.Conflict, "RRD databbase identified as ["+ path +"] was not found!")
          }
     } ~
      put { content(as[Root]) { data => context =>
         //val c = data.canonize
        val c = data
*/
      } ~
      detach{
        post {
          content(as[RRDRequest]) { query => context =>
            val db = _registry.getRRD4JBase(path, true)
            if (db != null) {
              val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              val fr = db.createFetchRequest(query.getFunction, query.getStart, query.getEnd, query.getResolution)
              val data = fr.fetchData()
              val out = new StringBuilder()
              val ts = data.getTimestamps
              val vs = data.getValues
              for (i <- 0 until data.getRowCount) {
                out append dateFormat.format(Util.getDate(ts(i)))
                vs.foreach{ v =>
                  out append "\t"
                  out append v(i).toString
                }
                out append "\n"
              }
              context complete (StatusCodes.OK, out.toString)
            }
            else {
               context fail (StatusCodes.Conflict, "RRD databbase identified as ["+ path +"] was not found!")
            }
          }
        }
      }

    } ~
    path("rrd" / "databases" / "[^/]+".r / "graph") { path =>
      get { parameters("start" ? "now", 'end ? "now", 'resolution ? "3600", 'funtion ? "AVERAGE", 'data) { (start, end, resolution, func, data) =>
            val db = _registry.getRRD4JBase(path, true)
            if (db != null) {
             val img = _registry.createDefaultGraph(path, data, start, end, resolution, func)

              _.complete("OK")
            }
            else {
               _.fail(StatusCodes.Conflict, "RRD databbase identified as ["+ path +"] was not found!")
            }
        }
      }
    }
  }
  
  private def buildUrl(ctx: RequestContext, path : String ) = { ctx.request.path  + "/"+ path  }

}