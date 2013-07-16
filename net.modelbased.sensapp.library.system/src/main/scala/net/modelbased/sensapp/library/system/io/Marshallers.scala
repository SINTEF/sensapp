/**
 * ====
 *     This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 *     Copyright (C) 2011-  SINTEF ICT
 *     Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 *     Module: net.modelbased.sensapp
 *
 *     SensApp is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as
 *     published by the Free Software Foundation, either version 3 of
 *     the License, or (at your option) any later version.
 *
 *     SensApp is distributed in the hope that it will be useful, but
 *     WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General
 *     Public License along with SensApp. If not, see
 *     <http://www.gnu.org/licenses/>.
 * ====
 *
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 * Module: net.modelbased.sensapp.library.system
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
package net.modelbased.sensapp.library.system.io

import cc.spray.typeconversion._
import cc.spray.http._
import cc.spray.http.MediaTypes._
import net.modelbased.sensapp.library.senml._
import net.modelbased.sensapp.library.senml.export._
import net.modelbased.sensapp.library.system.io.MediaTypes._

trait Marshaller {
  
  private[this] val targets = 
      ContentType(`application/json`) :: ContentType(`text/plain`) :: 
      ContentType(`text/xml`) :: ContentType(`senml+json`) :: 
      ContentType(`senml+xml`) :: Nil
  
  implicit lazy val RootMarshaller = new  SimpleMarshaller[Root] {
    override val canMarshalTo = targets
           
    override def marshal(value: Root, contentType: ContentType) = {
      contentType match {
        case ContentType(`text/plain`,_) | ContentType(`senml+json`, _) | ContentType(`application/json`, _) 
        	=> HttpContent(contentType, JsonParser.toJson(value))
        case ContentType(`senml+xml`, _) | ContentType(`text/xml`,_) 
        	=> HttpContent(contentType, (new scala.xml.PrettyPrinter(80, 2)).format(XmlParser.toXml(value)))
      }
    }
  }
  
  implicit lazy val MoPMarshaller = new  SimpleMarshaller[Seq[MeasurementOrParameter]] {
    override val canMarshalTo = targets
           
    override def marshal(value: Seq[MeasurementOrParameter], contentType: ContentType) = {
      val root = Root(None, None, None, None, if (value.isEmpty) None else Some(value))
      contentType match {
        case ContentType(`text/plain`,_) | ContentType(`senml+json`, _) | ContentType(`application/json`,_) 
        	=> HttpContent(contentType, JsonParser.toJson(root))
        case ContentType(`senml+xml`,_) | ContentType(`text/xml`,_) 
        	=> HttpContent(contentType, (new scala.xml.PrettyPrinter(80, 2)).format(XmlParser.toXml(root)))
      }
    }
  }
}


