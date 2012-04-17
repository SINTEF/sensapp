/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.library.sensors
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
package net.modelbased.sensapp.library.sensor

import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._

object JsonProtocol extends DefaultJsonProtocol {
  implicit val numberData = jsonFormat(NumberData, "n", "u", "t", "v")
  implicit val stringData = jsonFormat(StringData, "n", "u", "t", "sv")
  implicit val booleanData = jsonFormat(BooleanData,"n", "u", "t", "bv")
  implicit val summedData = jsonFormat(SummedData, "n", "u", "t", "s", "v")
}

