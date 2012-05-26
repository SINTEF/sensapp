/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
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
package net.modelbased.sensapp.library.system

import cc.spray.RequestContext

object URLHandler {

  def build(ctx: RequestContext, path: String): java.net.URL = {
    val host = ctx.request.host
    val port = ctx.request.port.getOrElse(8080)
    new java.net.URL("http", host, port, path)
  }
  
  def extract(str: String): ((String, Int),String) = {
    val url = new java.net.URL(str)
    val host = url.getHost
    val port = url.getPort
    val path = url.getPath
    ((host, port), path)
  }
}