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

import cc.spray._
import cc.spray.http._
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import net.modelbased.sensapp.repository.model.data._
import cc.spray.typeconversion.SprayJsonSupport
import net.modelbased.sensapp.repository.model.data.ModelJsonProtocol.modelFormat

trait ModelLister extends Directives with SprayJsonSupport {
      
  private[this] val _registry = new ModelRegistry()
  
  val service = {
    path("meta-models" / "repository" / "elements") {
      get  { ctx =>
        val uris = _registry.retrieve(List()) map { model => ctx.request.path  + "/"+ model.name }
        ctx.complete(uris)
      } ~
      content(as[Model]) { model =>
        post { ctx =>
          if (_registry exists(("name", model.name))) {
            ctx fail(StatusCodes.Conflict, "A model named ["+model.name+"] already exists!")
          } else {
        	_registry push(model)
        	ctx complete(StatusCodes.Created, ctx.request.path + "/"+ model.name) 
          } 
        }
      }
    }
  }
}