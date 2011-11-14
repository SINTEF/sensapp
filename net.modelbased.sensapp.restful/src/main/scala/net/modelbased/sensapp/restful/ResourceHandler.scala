/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.restful
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
package net.modelbased.sensapp.restful

import akka.actor.Actor
import akka.http._
import javax.ws.rs.core.MediaType

/**
 * A resource handler
 * @param pattern the URI schema that triggers this handler
 * @param request the actual requested URI
 */
abstract class ResourceHandler(val pattern: URIPattern, val request: String) 
   extends Actor {
  
  /**
   *  Contains the pass-by-uri parameters, declared in pattern
   */
  protected val _params: Map[String, String] = pattern.extract(request)
  
  /**
   * The receive method is an indirection to Sensapp dispatcher
   */
  def receive = {
    case r: RequestMethod => dispatch(r)
    case _ => throw new IllegalArgumentException
  }
  
  /**
   * A handler is a function of RequestMethod to Boolean. It implements 
   * the logic associated to a given method applied to a given resource
   */
  protected type Handler = RequestMethod => Boolean
  
  /**
   * Bindings have to be provided by the developer to bind a HTTP verb 
   * to a given handler
   */
  protected val _bindings: Map[String, Handler]
  
  /**
   * The dispatch method retrieve the handler associated to a request, 
   * and apply it to the request.
   * 
   * @param method the requestMethod received as invocation
   */
  private def dispatch(method: RequestMethod): Boolean = {
    val verb = method.request.getMethod()
    _bindings.get(verb)  match {
      case Some(handler) =>  handleRequest(handler, method)
      case None => handleNoHandler(verb, method)
    }
  }
  
  /**
   * Implements the behavior when no available handlers are found
   * 
   * @param verb the unhandled HTTP verb
   * @param method the request actually received
   */
  private def handleNoHandler(verb: String, method: RequestMethod): Boolean = {
    method.response.setContentType(MediaType.TEXT_PLAIN)
    method NotImplemented ("[" + verb + "] is not supported yet!")
  }
  
  /**
   * Apply a given handler to a given request. 
   * 
   * Exceptions are forwarded to the used as ServerError response
   * 
   * @param lambda the handler to be applied
   * @param method the receive request
   */
  private def handleRequest(lambda: Handler, method: RequestMethod): Boolean = {
    try { 
      lambda(method) 
    } 
    catch { 
      case e: Exception => { 
        method.response.setContentType(MediaType.TEXT_PLAIN)
        val msg = e.getMessage() 
        method Error msg
      }
    }
  }
}