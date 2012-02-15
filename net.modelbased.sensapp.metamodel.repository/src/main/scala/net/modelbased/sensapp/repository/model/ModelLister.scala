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