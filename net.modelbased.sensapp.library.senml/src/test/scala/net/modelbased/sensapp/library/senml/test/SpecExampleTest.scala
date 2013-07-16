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
 * Module: net.modelbased.sensapp.library.senml
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
package net.modelbased.sensapp.library.senml.test

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith

import net.modelbased.sensapp.library.senml.export.JsonParser
import net.modelbased.sensapp.library.senml.test.data.SenMLSpecificationExamples

@RunWith(classOf[JUnitRunner])
class SpecExampleTest extends SpecificationWithJUnit {

  "Test to handle the example from the standard".title
  
  
  "The JSON parser" should {
    "accepts the 'single data point' example [Section #6.1.1]" in {
      JsonParser.fromJson(SenMLSpecificationExamples.singleDatapoint) must not(throwAn[Exception])
    }
    "accepts the 'multiple data points' example (without time) [Section #6.1.2]" in {
      JsonParser.fromJson(SenMLSpecificationExamples.multipleDatapoint) must not(throwAn[Exception])
    }
    "accepts the 'multiple data points' example (with time) [Section #6.1.2]" in {
      JsonParser.fromJson(SenMLSpecificationExamples.multipleDatapointAndTime) must not(throwAn[Exception])
    }
    "accepts the 'multiple Measurements' example [Section #6.1.3]" in {
      JsonParser.fromJson(SenMLSpecificationExamples.multipleMeasurements) must not(throwAn[Exception])
    }
    "accepts the 'collection of resource' example [Section #6.1.4]" in {
      JsonParser.fromJson(SenMLSpecificationExamples.collectionOfResources) must not(throwAn[Exception])
    }
  }
  
  "The 'single data point example' [Section #6.1.1]" should {
    val obj = JsonParser.fromJson(SenMLSpecificationExamples.singleDatapoint)
    "contain only one measure" in { obj.measurementsOrParameters.size must_== 1 }
    "use 'urn:dev:ow:10e2073a01080063' as the measure resource name" in { obj.measurementsOrParameters.get(0).name must_== Some("urn:dev:ow:10e2073a01080063") }
    "use  23.5 as the measure value " in {  obj.measurementsOrParameters.get(0).value must_== Some(23.5) }
    "use 'A' as the measure unit" in { obj.measurementsOrParameters.get(0).units must_== Some("A") }
  }
    
}