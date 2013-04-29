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
package net.modelbased.sensapp.service.rrd.data

/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.service.rrd
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

import cc.spray.json._
import org.rrd4j.core.jrrd.ConsolidationFunctionType
import java.text.SimpleDateFormat
import org.rrd4j.core.Util
import org.rrd4j.core.timespec.TimeParser

/**
 * Created by IntelliJ IDEA.
 * User: franck
 * Date: 18/04/12
 * Time: 18:43
 * To change this template use File | Settings | File Templates.
 */

case class RRDCreateAndImport(path: String, data_url: String)
case class RRDCreateFromTemplate(path: String, template_url: String)
case class RRDRequest(function : String, start : String, end: String, resolution : String) {

  def getFunction() = {
     org.rrd4j.ConsolFun.valueOf(function)
  }

  def getStart() = convertTimestampToLong(start)
  def getEnd() = convertTimestampToLong(end)
  def getResolution() = resolution.toLong

  def convertTimestampToLong(t : String) : Long = {
    new TimeParser(t).parse().getTimestamp
  }

}
case class RRDTemplate(key: String, value: String)
case class RRDGraphTemplate(key: String, value: String)

object RRDJsonProtocol extends DefaultJsonProtocol {
    implicit val formatRRDCreateAndImport = jsonFormat(RRDCreateAndImport, "path", "data_url")
    implicit val formatRRDCreateFromTemplate = jsonFormat(RRDCreateFromTemplate, "path", "template_url")
    implicit val formatRRDRequest = jsonFormat(RRDRequest, "function", "start", "end", "resolution")
    implicit val formatRRDTemplate = jsonFormat(RRDTemplate, "key", "value")
    implicit val formatRRDGraphTemplate = jsonFormat(RRDGraphTemplate, "key", "value")
}