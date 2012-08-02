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
          context complete status
        }
      }
    }
  }
  
  def parseCSV(request : CSVDescriptor) : Root = {
    
    val reader : CSVReader = new CSVReader(new StringReader(request.raw), ',', '\\', 1);//skip headers
    val myEntries = reader.readAll();
      
    val dateFormat = new SimpleDateFormat(request.timestamp.format, new Locale(request.timestamp.locale))
         
    val raw = myEntries.toList/*.par*/.map{ line =>
      //println(line.mkString("[", ", ", "]"))
      try {
        val timestamp = dateFormat.parse(line(request.timestamp.columnId).trim).getTime() / 1000
        //println("Date: " + timestamp)
        val lineData : List[MeasurementOrParameter] = request.columns.map{col =>
          val data = line(col.columnId)
          //println("  Data: " + data)
          
          try {
            col.kind match {
              case "number" =>  Some(MeasurementOrParameter(Some(col.name), Some(col.unit), Some(data.toDouble), None, None, None, Some(timestamp), None))
              case "string" =>  Some(MeasurementOrParameter(Some(col.name), Some(col.unit), None, Some(data.trim), None, None, Some(timestamp), None))
              case "boolean" =>  Some(MeasurementOrParameter(Some(col.name), Some(col.unit), None, None, Some(data.trim=="true"), None, Some(timestamp), None))
              case "sum" =>  Some(MeasurementOrParameter(Some(col.name), Some(col.unit), None, None, None, Some(data.toDouble), Some(timestamp), None))
              case _ => 
                println("Kind does not exist, ignoring measurement " + col.name)
                None
            }
          } catch {
            case e : java.lang.NumberFormatException =>
              println("Cannot parse value, ignoring measurement " + col.name)
              None
          }
        }.flatten
        Some(lineData)
      } catch {
        case e : java.text.ParseException => 
          println("Cannot parse timestamp, ignoring line")
          None
      }    
    }.flatten.flatten
    
    println("Creating Root with " + raw.seq.size + " elements...")
    Root(Some(request.name+"/"), None, None, None, Some(raw.seq))
  }
}