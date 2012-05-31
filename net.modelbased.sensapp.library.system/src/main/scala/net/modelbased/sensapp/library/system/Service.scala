/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.library.system
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
package net.modelbased.sensapp.library.system

import cc.spray._
import cc.spray.http._
import cc.spray.directives._
import cc.spray.typeconversion.SprayJsonSupport
import cc.spray.http.HttpHeaders._
import cc.spray.encoding._

/** 
 * A SensApp service 
 * @author Sebastien Mosser
 */
trait Service extends Directives with SprayJsonSupport {
  
  // By default, we consider all the service on the same server
  val partners: PartnerHandler
  
  // the name of the service (to be used in the PartnerHandler)
  val name: String
  
  
  protected def cors(methods: String*) = {
    val allowed = "OPTIONS" :: methods.toList
    respondWithHeader(CustomHeader("Access-Control-Allow-Methods", allowed.mkString(", "))) {
      options { ctx => ctx complete "" }
    }
  }
  
  lazy val wrappedService: RequestContext => Unit = {
    (decodeRequest(Gzip) | decodeRequest(NoEncoding)) {
      respondWithHeader(CustomHeader("Access-Control-Allow-Origin", "*")) {
        jsonpWithParameter("callback") { 
          (encodeResponse(NoEncoding) | encodeResponse(Gzip)) {
            service 
          }
        }
      }
    }
  }
 
  // The actual service, described with the Spray DSL
  val service: RequestContext => Unit
  
  // the partners required by this service
  lazy val partnersNames: List[String] = List()
  
  private[this] def loadPartners() {
    partnersNames.foreach { n =>
      val p = partners(n)
      require(p != None, "Unknowm partner: [" + n + "] for service ["+name+"]")
      println("  # %s/%s --> %s:%s".format(name, n, p.get._1, p.get._2))
    }
  }
  loadPartners()  
}