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
package net.modelbased.sensapp.service.converter

import au.com.bytecode.opencsv.CSVReader
import cc.spray._
import cc.spray.http._
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import cc.spray.directives._
import cc.spray.typeconversion.SprayJsonSupport
import net.modelbased.sensapp.service.converter.request._
import net.modelbased.sensapp.service.converter.request.CSVDescriptorProtocols._

import java.io.StringReader
import java.text.SimpleDateFormat
import java.util.Locale
import net.modelbased.sensapp.library.system.{Service => SensAppService} 

import net.modelbased.sensapp.library.senml.Root
import net.modelbased.sensapp.library.senml.MeasurementOrParameter
import net.modelbased.sensapp.library.senml.export.JsonParser

import scala.collection.JavaConversions._


trait Service extends SensAppService {
  
  override implicit val partnerName = "converter"
  
  val service = {
    path("converter" / "fromCSV") {
      post {
        content(as[CSVDescriptor]) { request => context =>
          val status = parseCSV(request)
          print(status)
          context complete status
        }
      }
    }
  }
  
  def parseCSV(request : CSVDescriptor) : Root = {
    
    val reader : CSVReader = new CSVReader(new StringReader(request.raw), ',', '\\', 1);//skip headers
    val myEntries = reader.readAll();
      
    val dateFormat = new SimpleDateFormat(request.timestamp.format, new Locale(request.timestamp.locale))
      
    val raw = myEntries.toList.par.map{ line =>
      try {
        val timestamp = dateFormat.parse(line(request.timestamp.columnId)).getTime() / 1000
        println("Date: " + timestamp)
        val lineData = request.columns.map{col =>
          val data = line(col.columnId)
          println("  Data: " + data)
          MeasurementOrParameter(Some(col.name), Some(col.unit), None, Some(data), None, None, Some(timestamp), None)
        }
        Some(lineData)
      } catch {
        case e : java.text.ParseException => {
          println("Cannot parse timestamp, ignoring line")
          None
        }
      }    
    }.filter{ _.isDefined }.map{ _.get }.flatten
    
    Root(Some(request.name+"/"), None, None, None, Some(raw.seq))
  }
}