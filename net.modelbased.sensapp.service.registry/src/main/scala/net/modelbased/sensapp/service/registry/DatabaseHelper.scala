/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.service.registry
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
package net.modelbased.sensapp.service.registry

import net.modelbased.sensapp.service.registry.data._
import net.modelbased.sensapp.library.system.PartnerHandler
import cc.spray.client._
import cc.spray.json._
import cc.spray.typeconversion.SprayJsonSupport
import cc.spray.typeconversion.DefaultUnmarshallers._
import cc.spray.json.DefaultJsonProtocol._

/**
 * Helper to access to multiple database backend
 */
trait DatabaseHelper extends DefaultJsonProtocol with SprayJsonSupport {
  
  /**
   * Create a database in the specified backend
   * @param id the sensor identifier (will be used in the database)
   * @param schema the schema to be used by the database (backend, template, ...)
   * @param partner PartnerHandler to be used to access to the database
   * @return (descriptor, dataset) the URLs that respectively (i) describes and (ii) gives access to the database  
   */
  def createDatabase(id: String, schema: Schema , partners: PartnerHandler): (String, String)
  
  /**
   * Delete the backend database
   * @param description the backend information
   * @param partner PartnerHandler to be used to access to the database
   */
  def deleteDatabase(decription: Backend, partner: PartnerHandler)
  
}

/**
 * Object to be used as a factory to access to the right database helper
 * according to the requested type
 */
object BackendHelper {
  
  def apply(s: Schema): DatabaseHelper = get(s.backend)
  def apply(b: Backend):  DatabaseHelper = get(b.kind)
  
  def get(str: String): DatabaseHelper = {
    str match {
      case "raw" => Raw
      case _ => throw new RuntimeException("Unsuported Backend : [" + str + "]")
    }
  }
  
  /**
   * Database Helper for the Raw database
   */
  private object Raw extends DatabaseHelper  {
    
	case class CreationRequest (val sensor: String, val baseTime: Long, val schema: String)
	implicit val creationRequest = jsonFormat(CreationRequest, "sensor", "baseTime", "schema")
	
	def createDatabase(id: String, schema: Schema, partners: PartnerHandler): (String, String) = {
	  val request = schema.baseTime match {
	    case None => CreationRequest(id, (System.currentTimeMillis / 1000), schema.template)
	    case Some(bT) => CreationRequest(id, bT, schema.template)
	  }
	  val partner = partners("database.raw")
	  val conduit = new HttpConduit(partner._1,partner._2) {
	    val pipeline = simpleRequest[CreationRequest] ~> sendReceive
	  }
	  val response = conduit.pipeline(Post("/databases/raw/sensors", Some(request)))
	  val result = response.await.resultOrException
	  conduit.close()
	  result match {
	    case None => throw new RuntimeException("Unable to contact the raw backend service")
	    case Some(response) => {
	      response.status match {
	        case cc.spray.http.StatusCodes.Created => 
	          ( "/databases/raw/sensors/" + id, "/databases/raw/data/" + id )
	        case code => throw new RuntimeException("Raw backend service invocation failed ["+code+"]")
	      }
	    }
	  }
	}
	
	def deleteDatabase(backend: Backend, partners: PartnerHandler) {
	  val partner = partners("database.raw")
	  val conduit = new HttpConduit(partner._1,partner._2) {
	    val pipeline = simpleRequest ~> sendReceive
	  }
	  conduit.pipeline(Delete(backend.descriptor)).await.resultOrException
	  conduit.close()
	}
  } 
}