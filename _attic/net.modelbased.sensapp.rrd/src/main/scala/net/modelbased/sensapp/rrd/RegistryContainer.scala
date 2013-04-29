/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 * Module: net.modelbased.sensapp
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
package net.modelbased.sensapp.rrd

/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.registry
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
import net.modelbased.sensapp.restful._
import net.modelbased.sensapp.rrd.services._

class RegistryContainer extends Container {

  val helloPattern = new URIPattern("/sensapp-rrd/hello")
  val helloFactory = { req: String => new HelloWorld(helloPattern, req) }

  val templateRegistryPattern = new URIPattern("/sensapp-rrd/rrd-templates/{id:string}")
  val templateRegistryFactory = { req: String => new RRDTemplateRegistryService(templateRegistryPattern, req) }

  val templateListerPattern = new URIPattern("/sensapp-rrd/rrd-templates")
  val templateListerFactory = { req: String => new RRDTemplateListerService(templateListerPattern, req) }

  override val _registered = List(
      bind(helloPattern, helloFactory),
      bind(templateListerPattern, templateListerFactory),
      bind(templateRegistryPattern, templateRegistryFactory)
      )
}
