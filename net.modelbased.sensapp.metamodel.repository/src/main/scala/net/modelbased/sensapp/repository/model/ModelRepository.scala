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
package net.modelbased.sensapp.repository.model

import scala.xml.XML
import cc.spray._
import cc.spray.http._
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import cc.spray.typeconversion.SprayJsonSupport
import net.modelbased.sensapp.repository.model.data._
import net.modelbased.sensapp.repository.model.data.ModelJsonProtocol.modelFormat

trait ModelRepository extends Directives with SprayJsonSupport {
 
  val service = {
    path("meta-models" / "repository" / "elements" / "[^/]+".r) { name =>
      get  { ctx =>  
        handle(ctx, name, { m => ctx complete(XML.loadString(m.content)) })
      } ~
      delete { ctx =>
        handle(ctx, name, {m => _registry drop(m); ctx complete("true")})
      } ~
      content(as[Model]) { model =>
        put { ctx =>  
          if (model.name != name) {
            ctx fail(StatusCodes.Conflict, "Request content does not match URL for update")
          } else {
            handle(ctx, name, {m => _registry push(m); ctx complete("true") })
	      } 
        }
      }
    }
  }
  
  private[this] val _registry = new ModelRegistry()

  private def handle(ctx: RequestContext, name: String, action: Model => Unit) = {
    _registry pull(("name", name)) match {
      case None => ctx fail(StatusCodes.NotFound, "Unknown model ["+name+"]")
      case Some(model) => action(model)
    } 
  }
   
}