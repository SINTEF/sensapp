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

import net.modelbased.sensapp.library.system._

import java.io.StringReader
import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.Locale
import net.modelbased.sensapp.library.system.{Service => SensAppService} 

import net.modelbased.sensapp.library.senml.Root
import net.modelbased.sensapp.library.senml.MeasurementOrParameter
import net.modelbased.sensapp.library.senml.export.JsonParser

import scala.collection.JavaConversions._


trait Service extends SensAppService {
  
  override implicit val partnerName = "converter"
    
  private[this] val _registry = new CSVDescriptorRegistry()
  
  private def ifExists(context: RequestContext, name: String, lambda: => Unit) = {
    if (_registry exists ("desc", name))
      lambda
    else
      context fail(StatusCodes.NotFound, "Unknown descriptor [" + name + "]") 
  }
    
  val service = {
    path("converter" / "fromCSV") {
      post {
        content(as[CSVDescriptor]) { desc => context =>
          if (_registry exists ("name", desc.name)){
            context fail (StatusCodes.Conflict, "A CSV descriptor identified by ["+ desc.name +"] already exists!")
          } else {
            _registry push desc
            context complete (StatusCodes.Created, URLHandler.build("/converter/fromCSV/" + desc.name))
          }
        }
      } ~
      get { 
        parameter("flatten" ? false) { flatten =>  context =>
          val data = (_registry retrieve(List()))
          if (flatten) {
            context complete data
          } else {
            context complete (data map { r => URLHandler.build("/converter/fromCSV/" + r.name) })
          }
        }
      } ~ cors("GET", "POST")
    } ~
    path("converter" / "fromCSV" / "[a-zA-Z0-9][a-zA-Z0-9-:._/\\[\\]]*".r) { name =>
      get { context =>
        ifExists(context, name, {context complete (_registry pull ("desc", name)).get})
      } ~
      delete { context =>
        ifExists(context, name, {  
          val desc = (_registry pull ("desc", name)).get
          _registry drop desc 
          context complete "true"
        })
      } ~
      post { 
        content(as[String]) { rawData => context =>
          ifExists(context, name, {
            val desc = (_registry pull ("desc", name)).get
            val status = parseCSV(desc, rawData)
            context complete status
          })
        }
      } ~ 
      put {
        content(as[CSVDescriptor]) { desc => context => 
          if (desc.name != name) {
            context fail(StatusCodes.Conflict, "Request content does not match URL for update")
          } else {
            ifExists(context, name, { _registry push(desc); context complete desc })
	      } 
        }
      } ~ cors("GET", "PUT", "POST", "DELETE")
    }
  }
  
  //TODO: separator and escape character should me moved to (optional?) descriptor
  def parseCSV(request : CSVDescriptor, rawData : String) : Root = {
    
    val reader : CSVReader = new CSVReader(new StringReader(rawData), ',', '\\', 1);//skip headers
    val myEntries = reader.readAll();
      
    val getTimestamp: String => Option[Long] =  request.timestamp.format match {
        case None => (s: String) => try { 
          val t = s.toLong / 1000
          Some(t)
        } catch {case _ => None}
        case Some(format) => (s: String) => {
          val dateFormat = new SimpleDateFormat(format.pattern, new Locale(format.locale))
          try {
            val t = dateFormat.parse(s).getTime() / 1000
            Some(t)
          } catch {case _ => None}
        }
    }
    
    
          
    val raw = myEntries.toList/*.par*/.map{ line =>
      //println(line.mkString("[", ", ", "]"))
        getTimestamp(line(request.timestamp.columnId).trim) match {
          case Some(timestamp)=>
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
                  println("Kind " + col.kind + " does not exist, ignoring measurement " + col.name)
                  None
              }
            } catch {
              case e  =>
                println("Cannot parse value" + data +", ignoring measurement " + col.name)
                None
            }
            }.flatten 
            Some(lineData)
          case None => 
            println("Cannot parse timestamp, ignoring line\n\t" + line.mkString("[", ", ", "]"))
            None
        }}.flatten.flatten
    
    println("Creating Root with " + raw.seq.size + " elements...")
    Root(Some(request.name+"/"), None, None, None, Some(raw/*.seq*/))
  }
}


