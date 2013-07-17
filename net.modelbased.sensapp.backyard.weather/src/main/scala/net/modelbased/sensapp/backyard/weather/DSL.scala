/**
 * ====
 *     This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 *     Copyright (C) 2011-  SINTEF ICT
 *     Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 *     Module: net.modelbased.sensapp
 *
 *     SensApp is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as
 *     published by the Free Software Foundation, either version 3 of
 *     the License, or (at your option) any later version.
 *
 *     SensApp is distributed in the hope that it will be useful, but
 *     WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General
 *     Public License along with SensApp. If not, see
 *     <http://www.gnu.org/licenses/>.
 * ====
 *
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 * Module: net.modelbased.sensapp.backyard.weather
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
package net.modelbased.sensapp.backyard.weather

import net.modelbased.sensapp.backyard.weather.backend._
import net.modelbased.sensapp.library.senml.export.JsonParser

/**
 * The EKlima DSL
 * @author mosser
 */
trait EKlimaDSL {
  
  /**
   * A WeatherRequest is valid for a given station, between to dates ("YYY-MM-DD" format)
   * @param station the EKlima identifier for the station
   */
  protected class WeatherRequest(val station: Long) {
    var from: String = _
    var to: String = _
    
    /**
     * This method fills the current object with the date interval, and returns a new object
     * @param from starting date
     * @param to ending date
     */
    def between(interval: (String,String)): WeatherRequest = {
      from = interval._1
      to = interval._2
      this
    }
    
    /**
     * provide a textual representation of the current object
     */
    override def toString(): String = "*** " + station + " (" + from +"," + to + "): "
    
    /**
     * address the EKlima web service, transform the data into SenML, and store the result in a file
     * @param prefix the directory where the file will be created
     */
    def -> (prefix: String) {
      println(this + "retrieving data fron EKlima web service")
      val data = EKlima.getMetData(station, from, to, List("TAM","RR","RRTA"))
      println(this + "transforming data to senml")
      val senml = Xml2SenML(station.toString,data)
      val fileName = prefix + station + "_"+ from + "_" + to + ".senml.json"
      println(this + "writing output file: " + fileName)
      val outstream = new java.io.PrintStream(new java.io.FileOutputStream(new java.io.File(fileName)))
      outstream.print(JsonParser.toJson(senml))
      outstream.close()
      println(this + "done ")
    }
  }
  
  /**
   * transform a plain Long into a Weather Request
   */
  implicit def long2request(l: Long): WeatherRequest = new WeatherRequest(l)
}