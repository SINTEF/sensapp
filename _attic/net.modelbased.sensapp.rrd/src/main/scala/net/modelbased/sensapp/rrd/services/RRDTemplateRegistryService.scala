/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.rrd
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
package net.modelbased.sensapp.rrd.services

import net.modelbased.sensapp.restful._
import net.modelbased.sensapp.rrd.datamodel._
import akka.http._
import javax.ws.rs.core.MediaType
import java.io.{BufferedReader, InputStreamReader}
import org.specs2.internal.scalaz.Validation
import com.sun.xml.internal.ws.wsdl.writer.document.soap12.Body
import java.lang.StringBuilder
import org.rrd4j.core.RrdDefTemplate

/**
 * The service that exposes a set of XML RRD templates as a RESTful artefact
 *
 * The XML format used for the templates is the RRD4J format as described here:
 * http://rrd4j.googlecode.com/svn/trunk/javadoc/reference/org/rrd4j/core/RrdDefTemplate.html
 *
 * @author Sebastien Mosser
 * @author Franck Fleurey
 */
class RRDTemplateRegistryService(p: URIPattern, r: String) extends ResourceHandler(p,r) {
  
  // the internal registry
  private val _registry = new RRDTemplateRegistry()
  
  // The bindings expected as a ResourceHandler 
  override val _bindings = Map("GET"  -> { getTemplate(_) },
		  					   "PUT" -> { addTemplate(_) })
  
  /**
   * Retrieve an XML RRD template from the registry
   * 
   * <strong>Remark</strong>: A 404 status is returned if there is no template available
   * 
   * @param req the received request
   */
  private def getTemplate(req: RequestMethod) : Boolean  = {
    val identifier = _params("id")
    req.response.setContentType(MediaType.TEXT_PLAIN)

    _registry pull ("id", identifier) match {
      case Some(rrdtemplate) => req OK rrdtemplate.template
      case None => req NotFound ("RRD Template ["+identifier+"] not found.")
    }
  }
  
  /**
   * Add a RRD template into the registry, provided as an XML document.
   * The XML format is standard RRD4J XML format as described at
   * http://rrd4j.googlecode.com/svn/trunk/javadoc/reference/org/rrd4j/core/RrdDefTemplate.html
   * <strong>Remark</strong>:
   * <ul>
   * <li>The template is described using XML</li>
   * <li> A conflict (409) is returned if the description ID does not match the URL one
   * <li> An error is returned if the XML document cannot be parsed properly
   * </ul>
   */
  private def addTemplate(req: RequestMethod) : Boolean = {
    val id = _params("id")

    // Read the body of the request
    val br =  new BufferedReader(new InputStreamReader(req.request.getInputStream))
    val body = new StringBuilder
    while (br.ready()) body.append(br.readLine() + "\n")
    br.close();
    val xml = body.toString

    val rrdTemplate = new RRDTemplate(id, xml)
    req.response.setContentType(MediaType.TEXT_PLAIN)

    try {
      var rrdt = new RrdDefTemplate(xml)
       _registry push rrdTemplate
      req OK "OK"
    }
    catch {
      case e => req Error "Invalid Template: " + e.getMessage
    }
  }
}