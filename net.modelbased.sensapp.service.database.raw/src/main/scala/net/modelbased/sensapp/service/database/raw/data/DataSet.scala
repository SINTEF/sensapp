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

/**
 * The sensor database is modelled as a DataSet[E]
 * @param E the type of the data stored in this data set
 * @param sensor the sensor identifier (assumed unique)
 * @param baseTime the reference timestamp used in this dataset
 * @param data the data stored in the set so far
 * @param schema the schema associated to this dataset (\in {"Numerical", "String", "Boolean", "Summed", "NumericalStreamChunk"})
 * 
 */
case class DataSet[E](val sensor: String, val baseTime: Long, val data: List[E], val schema: String) {
  require(List("Numerical", "String", "Boolean", "Summed", "NumericalStreamChunk").contains(schema), "Unknown Schema")
}

/**
 * Supported DataEntry
 * this class is sealed, so all supported data entries must be defined in this file
 */
abstract sealed class DataEntry

/**
 * Numerical entry (float value)
 * @param delta the delta (in seconds) w.r.t. the reference time stamp in the data set
 * @param data the float value to be stored
 * @param unit the IANA unit associated to the value
 */
case class NumericalEntry(val delta: Long, val data: Double, val unit: String) extends DataEntry

/**
 * String entry 
 * @param delta the delta (in seconds) w.r.t. the reference time stamp in the data set
 * @param data the string value to be stored
 * @param unit the IANA unit associated to the value
 */
case class StringEntry(val delta: Long, val data: String, val unit: String) extends DataEntry

/**
 * Boolean entry
 * @param delta the delta (in seconds) w.r.t. the reference time stamp in the data set
 * @param data the boolean value to be stored
 */
case class BooleanEntry(val delta: Long, val data: Boolean) extends DataEntry

/**
 * Summed entry
 * @param delta the delta (in seconds) w.r.t. the reference time stamp in the data set
 * @param data the float value to be stored, implementing a summed data
 * @param unit the IANA unit associated to the value
 */
case class SummedEntry(val delta: Long, val data: Double, val unit: String, val instant: Option[Double]) extends DataEntry

/**
 * Numerical stream chunk
 * @param delta the delta (in seconds) w.r.t. the reference time stamp in the data set
 * @param update the update rate of the data set in this chunk
 * @param data the list of data that reifies a given chunk for this numerical string
 */
case class NumericalStreamChunkEntry(val delta: Long, val update: Int, val data: List[Option[Float]]) extends DataEntry

object DataSetProtocols extends DefaultJsonProtocol {
  implicit val numericalEntry = jsonFormat(NumericalEntry, "dt", "nd", "u")
  implicit val stringEntry = jsonFormat(StringEntry, "dt", "sd", "u")
  implicit val booleanEntry = jsonFormat(BooleanEntry, "dt", "bd")  
  implicit val summedEntry = jsonFormat(SummedEntry, "dt", "bd", "u", "d")
  implicit val numericalStreamChunkEntry = jsonFormat(NumericalStreamChunkEntry, "dt", "u", "num_lst")
  implicit def dataSet[A : JsonFormat] = jsonFormat(DataSet.apply[A], "s", "bt", "e", "k") 
}
