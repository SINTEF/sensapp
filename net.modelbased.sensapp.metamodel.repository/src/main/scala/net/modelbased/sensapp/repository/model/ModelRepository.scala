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