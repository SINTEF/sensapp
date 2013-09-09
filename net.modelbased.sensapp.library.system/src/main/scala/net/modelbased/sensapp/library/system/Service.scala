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
trait Service extends Directives with  io.Marshaller with io.Unmarshaller with SprayJsonSupport {

  /****************************************
   ** To be filled by service developers **
   ****************************************/
  
  implicit val partners: PartnerHandler                  // By default, we consider all the service on the same server
  implicit val partnerName: String                     // the name of the service (to be used in the PartnerHandler)
  val service: RequestContext => Unit           // The actual service, described with the Spray DSL
  lazy val partnersNames: List[String] = List() // the partners required by this service
  
  /****************************************************************
   *  Internal methods and value used by SensApp service library **
   ****************************************************************/
  
  /**
   * headers to be sent for **any** response returned by SensApp services
   */
  private val headers: Map[String, Seq[String]] = Map(
      "Access-Control-Allow-Origin" -> Seq("*"),
      "Access-Control-Allow-Headers" -> Seq("Accept", "Content-Type")
    )
  
  /**
   * the actual service to be executed, handling encoding, decoding, headers, JSON-P, ... 
   */
  lazy val wrappedService: RequestContext => Unit = {
    val headers = this.headers.toList map { case (h,vals) => CustomHeader(h, vals.mkString(", ")) }
    (decodeRequest(Gzip) | decodeRequest(NoEncoding)) {
      respondWithHeaders(headers: _*) {
        jsonpWithParameter("callback") { 
          (encodeResponse(NoEncoding) | encodeResponse(Gzip)) { service }
        }
      }
    }
  }
  
  /**
   * describe methods available for CORS support
   */
  protected def cors(methods: String*) = {
    val allowed = "OPTIONS" :: methods.toList
    respondWithHeader(CustomHeader("Access-Control-Allow-Methods", allowed.mkString(", "))) {
      options { ctx => ctx complete "" }
    }
  }
  
  /**
   * check that all needed partner is given
   */
  private[this] def loadPartners() {
    partnersNames.foreach { n =>
      val p = partners(n)
      require(p != None, "Unknowm partner: [" + n + "] for service ["+partnerName+"]")
      println("  # %s/%s --> %s:%s".format(partnerName, n, p.get._1, p.get._2))
    }
  }
  loadPartners()  
}