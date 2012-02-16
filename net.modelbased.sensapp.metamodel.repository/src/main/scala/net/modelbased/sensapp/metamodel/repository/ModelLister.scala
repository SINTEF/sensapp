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

import cc.spray._
import cc.spray.http._
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import net.modelbased.sensapp.metamodel.repository.data._
import cc.spray.typeconversion.SprayJsonSupport
import net.modelbased.sensapp.metamodel.repository.data.ModelJsonProtocol.modelFormat

/**
 * A meta-model lister for SensApp.
 * 
 * It reacts to the /meta-models/repository/elements url
 *   - GET returns the list of URL related to the stored models
 *   - POST store the received meta-model in the repository
 * 
 * @author Sebastien Mosser
 */
trait ModelLister extends Directives with SprayJsonSupport {
  
  /**
   * The service implemented in this trait
   */
  val service = {
    path("meta-models" / "repository" / "elements") {
      get  { ctx =>
        val uris = _registry.retrieve(List()) map { model => ctx.request.path  + "/"+ model.name }
        ctx.complete(uris)
      } ~
      post {
       content(as[Model]) { model => ctx =>
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
  
  // The internal registry used as a storage back-end
  private[this] val _registry = new ModelRegistry()
}