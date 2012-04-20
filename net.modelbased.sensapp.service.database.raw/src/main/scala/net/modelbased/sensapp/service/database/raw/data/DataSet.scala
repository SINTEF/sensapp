/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.service.database.raw
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
package net.modelbased.sensapp.service.database.raw.data

import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol

case class DataSet[E](val sensor: String, val baseTime: Long, val data: List[E], val schema: String) 

abstract sealed class DataEntry
case class NumericalEntry(val delta: Long, val data: Float, val unit: String) extends DataEntry
case class StringEntry(val delta: Long, val data: String, val unit: String) extends DataEntry
case class BooleanEntry(val delta: Long, val data: Boolean) extends DataEntry
case class SummedEntry(val delta: Long, val data: Float, val unit: String, val instant: Option[Float]) extends DataEntry
case class NumericalStreamChunkEntry(val delta: Long, val data: List[Float]) extends DataEntry

object DataSetProtocols extends DefaultJsonProtocol {
  implicit val numericalEntry = jsonFormat(NumericalEntry, "dt", "nd", "u")
  implicit val stringEntry = jsonFormat(StringEntry, "dt", "sd", "u")
  implicit val booleanEntry = jsonFormat(BooleanEntry, "dt", "bd")  
  implicit val summedEntry = jsonFormat(SummedEntry, "dt", "bd", "u", "d")
  implicit val numericalStreamChunkEntry = jsonFormat(NumericalStreamChunkEntry, "dt", "num_lst")
  implicit def dataSet[A : JsonFormat] = jsonFormat(DataSet.apply[A], "s", "bt", "e", "k") 
}
