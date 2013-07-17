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
package net.modelbased.sensapp.backyard.weather.backend

import scala.xml.XML
import java.text.SimpleDateFormat
import java.util.Date
import net.modelbased.sensapp.library.senml._

object Xml2SenML {
  
  /**
   * Data tuple retrieved from EKlima
   */
  private case class Data(date: String, element: String, value: String)
  
  /**
   * transform the XML data retrieved from EKlima into plain SenML
   * It automatically ignore irrelevant data (valued as -99999 in EKlima)
   * @param sensor the sensor ID to be used in the SenML document
   * @param raw the XML element retrieved from EKlima
   */
  def apply(sensor: String, raw: scala.xml.Elem) = {
    val data: Seq[Data] = extractData(raw)
    val filtered = (data.par filterNot { _.value == "-99999" }).seq
    val senml = build(sensor, filtered)
    senml
  }
 
  /**
   * Extract the Weather data from the SOAP envelope obtained as EKLima answer
   * @param root the SOAP message to be used
   * @return 
   */
  private def extractData(root: scala.xml.Elem): Seq[Data] =  {
    val items = root \ "Body" \ "getMetDataResponse" \ "return" \ "timeStamp" \ "item"
    val par = items.par flatMap { i => 
      val date = (i \ "from").text
      val items = i \ "location" \ "item" \ "weatherElement" \ "item"
      items map { measure => 
        val element = (measure \ "id").text
        val value = (measure \ "value").text
        Data(date, element, value) 
      }
    }
    par.seq
  }
    
  /**
   * build the SenML root element associated to a given sequence of EKlima data
   * @param sensor the name of the sensor (used as baseName in the SenML root)
   * @param data the sequence of data to transform
   * @return a SenML Root instance
   */
  private def build(sensor: String, data: Seq[Data]): Root = {
    if (data.isEmpty) 
      return Root(Some("eklima-" + sensor+ "/"), None, None, None, None)
    val mop = data.par map {d => 
      val value = adapt(d.value.toFloat, d.element)
      val unit = units(d.element)
      val timestamp = stringTotimestamp(d.date)
      MeasurementOrParameter(Some(d.element), Some(unit), Some(value),None, None, None, Some(timestamp), None)
    }
    Root(Some("eklima-" + sensor+ "/"), None, None, None, Some(mop.seq))
  }
  
  /**
   * transform a XML data into a timestamp in seconds
   * @param str the string to be transformed
   * @return the timestamp (numbers of seconds since EPOCH)
   */
  private def stringTotimestamp(str: String): Long = {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val date = format.parse(str)
    date.getTime() / 1000 // (from milliseconds to seconds)
  }
  
  /**
   * adapter function, used to adapt EKlima values to SenML units 
   * (e.g., precipitation expressed in millimeters for Eklima, but in meters in SenML)
   * @param value the value to adapt
   * @param the kind (EKlima element code) associated to this value
   */
  private def adapt(value: Float, kind: String) = kind match {
    case ("RRTA" | "RR") => value / 1000 // Milimeters to meters
    case _ => value
  }
  
  /**
   * internal map to bind EKlima weather elements to IANA unit codes
   */
  private[this] val units = Map("RRTA" -> "m", "RR" -> "m", "TAM" -> "degC")
  
}