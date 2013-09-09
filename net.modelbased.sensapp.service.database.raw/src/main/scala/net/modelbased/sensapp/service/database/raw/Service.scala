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
 * Module: net.modelbased.sensapp.service.database.raw
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
package net.modelbased.sensapp.service.database.raw

import cc.spray._
import cc.spray.http._
import cc.spray.typeconversion.SprayJsonSupport
import net.modelbased.sensapp.library.system.{Service => SensAppService, URLHandler}
import net.modelbased.sensapp.library.senml.{Root => SenMLRoot}
import net.modelbased.sensapp.library.senml.spec.{Standard => SenMLStd}
import net.modelbased.sensapp.service.database.raw.data._
import net.modelbased.sensapp.service.database.raw.backend.Backend
import net.modelbased.sensapp.service.database.raw.backend.impl.MongoDB
import cc.spray.json.DefaultJsonProtocol._

trait RawDatabaseService extends SensAppService {

  override implicit val partnerName = "database.raw"
    
  private[this] val _backend: Backend = new MongoDB()
   
  import RequestsProtocols._
  
  val service = {
    path("databases" / "raw" / "sensors") { 
      get { 
        parameter("flatten" ? false) { flatten => context =>
            if (!flatten) {
              val uris = _backend.content map { s => URLHandler.build("/databases/raw/sensors/"+ s).toString }
	          context complete uris
            } else {
              val dataset = _backend.content map { s => _backend.describe(s, URLHandler.build("/databases/raw/sensors/").toString).get }
              context complete dataset
            }
        }
      } ~
      post {  
        content(as[CreationRequest]) { req => context =>
          if (_backend exists req.sensor){
            context fail (StatusCodes.Conflict, "A sensor database identified as ["+ req.sensor +"] already exists!")
          } else {
            _backend create req
            context complete(StatusCodes.Created, URLHandler.build("/databases/raw/sensors/"+ req.sensor).toString )
          }
        }
      } ~ cors("GET", "POST")
    } ~
    path("databases" / "raw" / "sensors" / SenMLStd.NAME_VALIDATOR.r ) { name => 
      get { context => 
        ifExists(context, name, {
          val description = _backend describe(name, URLHandler.build("/databases/raw/data/").toString)
          context complete description
        })
      } ~
      delete { context => 
        context complete (_backend delete name).toString
      } ~ cors("GET", "DELETE")
    } ~
    detach {
      path ("databases" / "raw" / "load") {
        put { 
          content(as[SenMLRoot]) { root => context =>
            try {
              val start = System.currentTimeMillis()
              _backend importer root
              val delta = System.currentTimeMillis() - start
              context complete "processed in %sms".format(delta)
            } catch {
              case e => context complete (StatusCodes.InternalServerError, "" + e.toString())
            }
          }
        } ~ cors("PUT")
      } ~
      path ("databases" / "raw" / "data") {
	      post { 
	        content(as[SearchRequest]) { request => context =>
	          val from = buildTimeStamp(request.from)
	          val to = buildTimeStamp(request.to)
	          val sort = request.sorted.getOrElse("none")
	          val limit = request.limit.getOrElse(-1)
	          val existing = request.sensors.par.filter{ _backend exists(_) }
	          context complete (_backend get(existing.seq, from, to, sort, limit))
	        }
	      } ~ cors("POST")
	    } ~
	    path("databases" / "raw" / "data" / SenMLStd.NAME_VALIDATOR.r ) { name => 
	      get { 
	        parameters("from" ? "0", "to" ? "now", "sorted" ? "none", "limit" ? -1, "factorized" ? false, "every" ? 1, "by" ? "avg") { (from, to, sorted, limit, factorized, every, by) => context =>
	          ifExists(context, name, {
	            val dataset = (_backend get(name, buildTimeStamp(from), buildTimeStamp(to), sorted, limit)).sampled(every, by).head
	            context complete (if (factorized) dataset.factorized.head else dataset)
	          })
	        } 
	      } ~
	      put { 
	        content(as[SenMLRoot]) { raw => context =>
	          ifExists(context, name, { context complete (_backend push (name, raw)) })
	        }
	      } ~ cors("GET", "PUT")
	    }
    }
  }
 
  private def buildTimeStamp(str: String): Long = {
    import java.text.SimpleDateFormat
    import java.util.Date
    val TimeStamp = """(\d+)""".r
    val LiteralDate = """(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d)""".r
    val Now = "now"
    str match {
      case Now => (System.currentTimeMillis() / 1000)
      case TimeStamp(x) => x.toLong
      case LiteralDate(lit)   => {
        val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
        val date = format.parse(lit)
        date.getTime() / 1000
      }
      case _ => throw new RuntimeException("Unable to parse date ["+str+"]!")
    }  
  }
  
  private def ifExists(context: RequestContext, name: String, lambda: => Unit) = {
    if (_backend exists name)
      lambda
    else
      context fail(StatusCodes.NotFound, "Unknown sensor database [" + name + "]") 
  } 
}