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
package net.modelbased.sensapp.service.sample

import au.com.bytecode.opencsv.{CSVReader}

import java.io.FileReader

import java.text.{DateFormat, SimpleDateFormat}

import cc.spray._
import cc.spray.http._
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import cc.spray.directives._
import cc.spray.typeconversion.SprayJsonSupport

import scala.collection.JavaConversions._


import java.util.Locale
import net.modelbased.sensapp.library.system.{Service => SensAppService} 

trait Service extends SensAppService {
  
  override implicit val partnerName = "service.sample"
  
  val service = {
    path("sample" / "elements") {
      get { _ complete "foo" }
    }}
}


object TestParser {
    
  def main(args : Array[String]) {
      val reader : CSVReader = new CSVReader(new FileReader("C:/work/OBD/trackLog-2012-Aug-01_08-46-13.csv"), ',', '\\', 1);
      val myEntries = reader.readAll();
      
      val dateFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss z yyyy", Locale.US)

      myEntries.foreach{line =>
        println("DATE:" + line(0))
        try {
          println("DATE: " + dateFormat.parse(line(0)))
        } catch {
          case e : java.text.ParseException => println("Cannot parse timestamp, ignoring line")
        }
        println(line.mkString("[", ", ", "]"))
      }
  }
   
  
  
}