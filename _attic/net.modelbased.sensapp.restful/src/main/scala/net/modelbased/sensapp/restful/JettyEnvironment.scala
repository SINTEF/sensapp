/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.restful
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
package net.modelbased.sensapp.restful

import org.specs2.mutable._
import org.eclipse.jetty.server.Server

/**
 * A Trait to embed te current webapp in a Jetty server
 * 
 * Useful to test RESTful services
 * 
 * @author Sebastien Mosser
 */
trait JettyEnvironment {

  /**
   * The port used to run the Jetty Server
   */
  protected var serverPort = 8090
  
  /**
   * The context used to expose the project
   */
  protected var context = "/"
    
  // the internal server
  private val _server = new Server(serverPort)
  
  /**
   * Start the Jetty server
   */
  def jettyStart() {
    _server.setHandler(new org.eclipse.jetty.webapp.WebAppContext("src/main/webapp",context))
    _server.start()
  }
  
  /**
   * Stop the Jetty server
   */
  def jettyStop() { _server.stop() }
  
}