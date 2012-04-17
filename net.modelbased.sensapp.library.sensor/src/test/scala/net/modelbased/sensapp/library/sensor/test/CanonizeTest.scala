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
package net.modelbased.sensapp.library.sensor.test

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import net.modelbased.sensapp.library.sensor._
import net.modelbased.sensapp.library.senml.{JsonParser => SenMLParser}
import net.modelbased.sensapp.library.sensor.JsonProtocol._

@RunWith(classOf[JUnitRunner])
class CanonizeTest extends SpecificationWithJUnit {

  "Test for the canonize operation".title
  
  "The Single data point example [SenML spec, Section #6.1.1]" should {
    val json = """
      {"e":[{ "n": "urn:dev:ow:10e2073a01080063", "v":23.5 , "u": "A"}]}
    """
    val root = SenMLParser.fromJson(json)
    val mOp = root.measurementsOrParameters(0)
    
    "be accepted as input" in { SensorMeasureBuilder.canonize(root) must not(throwAn[Exception]) }
    
    val measureList = SensorMeasureBuilder.canonize(root)
    "contains a single SensorMeasure" in { measureList.size must_== 1 }
    "accept this measure as a NumberData" in { measureList(0).asInstanceOf[NumberData] must not(throwAn[Exception])}
    val x = 'toto
   
    val measure = SensorMeasureBuilder.canonize(root)(0).asInstanceOf[NumberData]
    "contains a measure named 'urn:dev:ow:10e2073a01080063'" in { measure.name must_== "urn:dev:ow:10e2073a01080063"}
    "contains a measure valued 23.5" in { measure.data must_== 23.5 }
    "contains a measure with unit 'A'" in { measure.unit must_== "A" }
    "contains a measure with time value" in { measure.time must_!= 0}
       
  }
  
  
}