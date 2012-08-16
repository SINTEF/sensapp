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
package net.modelbased.sensapp.service.converter.request

import cc.spray.json._

/**
 * The following classes and object deal with the import of CSV file into SensApp datasets
 */
case class  CSVDescriptor(val name: String, 
    val timestamp: TimeStampDescriptor, 
    val columns: List[ColumnDescriptor],
    val separator: Option[Char],
    val escape: Option[Char],
    val locale : Option[String],
    val baseName : Option[String])


case class DateFormatDescriptor(val pattern: String, val locale : String)

case class TimeStampDescriptor(val columnId: Int, val format: Option[DateFormatDescriptor])

case class ColumnDescriptor(val columnId: Int, val name: String, val unit: String, val kind : String, val strategy : Option[String]){
  require(List("number", "string", "boolean", "sum").contains(kind), "invalid kind")
  require(strategy.isDefined && (kind=="number" && List("chunk", "min", "max", "avg", "one").contains(strategy.get)) || !strategy.isDefined, "invalid strategy")
}


//TODO: we should probably merge the 2 locale (for the data and for the date...)
object CSVDescriptorProtocols extends DefaultJsonProtocol {
  implicit val dateFormatFormat = jsonFormat(DateFormatDescriptor, "pattern", "locale")
  implicit val timestampDescriptorFormat = jsonFormat(TimeStampDescriptor, "colId", "format")
  implicit val columnDescriptorFormat = jsonFormat(ColumnDescriptor,"colId", "name", "unit", "kind", "strategy")
  implicit val csvDescriptorFormat = jsonFormat(CSVDescriptor,"desc", "timestamp", "columns", "separator", "escape", "locale", "bn")
}

/**
 * The following classes and object deal with the export of SensApp datasets to CSV
 */
case class DataSetDescriptor(val url: String, val as: String, val unroll : Option[Boolean])

case class CSVExportDescriptor(val datasets : List[DataSetDescriptor], val separator : Option[String])

object CSVExportDescriptorProtocols extends DefaultJsonProtocol {
  implicit val dataSetFormat = jsonFormat(DataSetDescriptor, "url", "as", "unroll")
  implicit val csvExportFormat = jsonFormat(CSVExportDescriptor, "datasets", "separator")
}