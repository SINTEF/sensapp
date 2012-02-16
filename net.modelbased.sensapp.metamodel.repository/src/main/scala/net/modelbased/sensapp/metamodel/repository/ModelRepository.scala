/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.metamodel.repository
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
package net.modelbased.sensapp.metamodel.repository

import scala.xml.XML
import cc.spray._
import cc.spray.http._
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import cc.spray.typeconversion.SprayJsonSupport
import net.modelbased.sensapp.metamodel.repository.data._
import net.modelbased.sensapp.metamodel.repository.data.ModelJsonProtocol.modelFormat

/**
 * A meta-model repository for SensApp.
 * 
 * It reacts to the /meta-models/repository/elements/{NAME} url
 *   - GET returns the XML description of the {NAME} meta-model
 *   - DELETE deletes the {NAME} meta-model
 *   - PUT update the meta-model description
 * 
 * @author Sebastien Mosser
 */
trait ModelRepository extends Directives with SprayJsonSupport {
 
  /**
   * The service implemented in this trait
   */
  val service = {
    path("meta-models" / "repository" / "elements" / "[^/]+".r) { name =>
      get  { ctx =>  
        handle(ctx, name, { m => ctx complete(XML.loadString(m.content)) })
      } ~
      delete { ctx =>
        handle(ctx, name, {m => _registry drop(m); ctx complete("true")})
      } ~
      put { 
        content(as[Model]) { model => ctx =>
          if (model.name != name) {
            ctx fail(StatusCodes.Conflict, "Request content does not match URL for update")
          } else {
            handle(ctx, name, {m => _registry push(m); ctx complete("true") })
	      } 
        }
      }
    }
  }
  
  // The internal registry used as a storage back-end
  private[this] val _registry = new ModelRegistry()

  /**
   * Handle a given request
   * 
   * If the requested model does not exists, it answers a 404 (not found) response.
   * 
   * @param ctx the request context associated to this request
   * @param name the name of the requested meta-model
   * @param action an anonymous function than handles the retrieved model
   */
  private def handle(ctx: RequestContext, name: String, action: Model => Unit) = {
    _registry pull(("name", name)) match {
      case None => ctx fail(StatusCodes.NotFound, "Unknown model ["+name+"]")
      case Some(model) => action(model)
    } 
  }
   
}