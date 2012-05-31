/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.service.converter
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
package net.modelbased.sensapp.service.converter

import cc.spray._
import cc.spray.typeconversion.SprayJsonSupport
import net.modelbased.sensapp.library.senml.Root
import net.modelbased.sensapp.library.senml.export._
import net.modelbased.sensapp.library.system.{Service => SensAppService}
import typeconversion._
import cc.spray.http._
import MediaTypes._
import HttpHeaders._
import SensAppMediaType._

object SensAppMediaType {
    val `senml+xml` = register(CustomMediaType("application/senml+xml", "senml+xml"))
    val `senml+json` = register(CustomMediaType("application/senml+json", "senml+json"))
}

trait SenMLMarshalling {
  implicit lazy val XmlMarshaller = new  SimpleMarshaller[Root] {
    override val canMarshalTo = ContentType(`senml+xml`) :: ContentType(`text/xml`) :: Nil
    override def marshal(value: Root, contentType: ContentType) = {
      HttpContent(contentType, XmlParser.toXml(value).toString)
    }
  }
  
  implicit lazy val JsonMarshaller = new  SimpleMarshaller[Root] {
    override val canMarshalTo = ContentType(`senml+json`) :: ContentType(`application/json`) :: Nil
    override def marshal(value: Root, contentType: ContentType) = {
      HttpContent(contentType, JsonParser.toJson(value))
    }
  }
  
  implicit lazy val JsonUnmarshaller = new SimpleUnmarshaller[Root] {
    override val canUnmarshalFrom = ContentTypeRange(`senml+json`) :: ContentTypeRange(`application/json`) :: Nil
    override def unmarshal(content: HttpContent) = protect {
      JsonParser.fromJson(DefaultUnmarshallers.StringUnmarshaller(content).right.get)
    }
  }  
}


object SensorDataMarshalling extends SenMLMarshalling

trait Service extends SensAppService {
  import SensorDataMarshalling._
  override lazy val name = "converter"
  val service = path("converter") { post { content(as[String]) { data => context => context.complete(data)}} ~ cors("POST") }
}