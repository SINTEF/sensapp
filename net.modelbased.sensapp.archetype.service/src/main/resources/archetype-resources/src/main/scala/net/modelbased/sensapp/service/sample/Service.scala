/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.archetype.service
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
package net.modelbased.sensapp.service.sample

import cc.spray._
import cc.spray.http._
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import cc.spray.directives._
import cc.spray.typeconversion.SprayJsonSupport
// Application specific:
import net.modelbased.sensapp.service.sample.data.{Element, ElementRegistry }
import net.modelbased.sensapp.service.sample.data.ElementJsonProtocol.format


trait Service extends Directives with SprayJsonSupport {
  
  val service = {
    path("sample" / "elements") {
      get { context =>
        val uris = (_registry retrieve(List())) map { buildUrl(context, _) }
        context complete uris
      } ~
      post {
        content(as[Element]) { element => context =>
          if (_registry exists ("key", element.key)){
            context fail (StatusCodes.Conflict, "An Element identified as ["+ element.key +"] already exists!")
          } else {
            _registry push element
            context complete (StatusCodes.Created, buildUrl(context, element))
          }
        }
      }
    } ~
    path("sample" / "elements" / IntNumber) { key =>
      get { context =>
        handle(context, key, { context complete _})
      } ~
      delete { context =>
        handle(context, key, { e => _registry drop e; context complete "true"})
      } ~
      put {
        content(as[Element]) { element => context => 
          if (element.key != key) {
            context fail(StatusCodes.Conflict, "Request content does not match URL for update")
          } else {
            handle(context, key, {e => _registry push(element); context complete("true") })
	      } 
        }
      }
    }
  }
  
  private[this] val _registry = new ElementRegistry()
  
  private def buildUrl(ctx: RequestContext, e: Element) = { ctx.request.path  + "/"+ e.key  }
  
  private def handle(ctx: RequestContext, key: Int, action: Element => Unit) = {
    _registry pull(("key", key)) match {
      case None => ctx fail(StatusCodes.NotFound, "Unknown element ["+key+"]")
      case Some(element) => action(element)
    } 
  }
}