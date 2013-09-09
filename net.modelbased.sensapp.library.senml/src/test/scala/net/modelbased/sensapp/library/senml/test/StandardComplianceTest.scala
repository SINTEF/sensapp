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
import org.specs2.matcher.DataTables
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith

import net.modelbased.sensapp.library.senml.export.JsonParser


@RunWith(classOf[JUnitRunner])
class StandardComplianceTest extends SpecificationWithJUnit with DataTables{

  "Error checking capabilities of the SenML library".title

  import net.modelbased.sensapp.library.senml.spec.RootComplianceChecker._ 

  
  "Root object" should {
    "rejects a version number lesser than 0" in { check("""{"ver": -1, "e":[{ "n": "myname", "v": 0.0, "u": "m" }]}""", UNSUPPORTED_VERSION) }
    "rejects an unsupported version number " in { check("""{"ver": 0, "e":[{ "n": "myname", "v": 0.0, "u": "m" }]}""", UNSUPPORTED_VERSION) }
    "rejects an unknown baseUnit"            in { check("""{"bu": "myUnknownUnitCode", "e":[{ "n": "myname", "v": 0.0,  "u": "m" }]}""", UNKNOWN_BASE_UNIT) }
    "rejects a measure without unit"         in { check("""{"e":[{ "n": "myname", "v": 0.0}]}""", ALL_UNITS_DEFINED) }
    "accepts a boolean value without unit"   in { 
      val json = """{"e":[{ "n": "myname", "bv": "true"}]}"""
      JsonParser.fromJson(json) must not(throwAn[IllegalArgumentException])
    }
    "rejects an empty measurement entry"     in { check("""{"e":[]}""", EMPTY_MEASUREMENTS) }
    "rejects a measure with an unknown unit" in { check("""{"e":[{ "n": "myname", "v": 0.0, "u": "myUnknownUnitCode" }]}""", UNKNWOWN_UNIT) }
    "rejects a badly formed baseName"        in { check("""{"bn": "/myname", "e":[{ "v": 0.0, "u": "m" }]}""", INVALID_NAME) }
    "rejects an anonymous measurement"       in { check("""{"e":[{ "v": 0.0,  "u": "m" }]}""", INVALID_NAME) }
    "rejects a badly formed name"            in { check("""{"e":[{ "n": "/name", "v": 0.0, "u": "m" }]}""", INVALID_NAME) }
    
    "rejects a badly valued measure"         in { 
      """s"""        || """v"""        | """sv"""          | """bv"""         |
      """"""         !! """"""         ! """"""            ! """"""           |
      """"""         !! """"""         ! """"sv": "val"""" ! """"bv": true""" |
      """"""         !! """"v": 1.0""" ! """"""            ! """"bv": true""" |
      """"""         !! """"v": 1.0""" ! """"sv": "val"""" ! """"""           |
      """"""         !! """"v": 1.0""" ! """"sv": "val"""" ! """"bv": true""" |
      """"s": 1.0""" !! """"""         ! """"sv": "val"""" ! """"bv": true""" |
      """"s": 1.0""" !! """"v": 1.0""" ! """"""            ! """"bv": true""" |
      """"s": 1.0""" !! """"v": 1.0""" ! """"sv": "val"""" ! """"""           |
      """"s": 1.0""" !! """"v": 1.0""" ! """"sv": "val"""" ! """"bv": true""" |> { (s,v,sv,bv) =>
        check(buildJson(s,v,sv,bv), AMBIGUOUS_VALUE_PROVIDED)
      }       
    }
    
    "accepts ritghly valued measures" in {
      """s"""        || """v"""        | """sv"""          | """bv"""         |
      """"""         !! """"""         ! """"""            ! """"bv": true""" |
      """"""         !! """"""         ! """"sv": "val"""" ! """"""           |
      """"""         !! """"v": 1.0""" ! """"""            ! """"""           |
      """"s": 1.0""" !! """"""         ! """"""            ! """"""           |
      """"s": 1.0""" !! """"""         ! """"""            ! """"bv": true""" |
      """"s": 1.0""" !! """"""         ! """"sv": "val"""" ! """"""           |
      """"s": 1.0""" !! """"v": 1.0""" ! """"""            ! """"""           |> { (s,v,sv,bv) =>
        JsonParser.fromJson(buildJson(s,v,sv,bv)) must not(throwAn[IllegalArgumentException]())
      }       
    }
  }
  
  private def buildJson(s: String, v: String, sv: String, bv: String) = {
    val data = List(s,v,sv,bv).filterNot( _ == "")
    if (data.isEmpty)
      """{"e": [{ "n": "name", "u": "m"}]}"""
    else
      data.mkString("""{"e": [{ "n": "name", "u": "m", """, ", ", "}]}")
  }
  
  private def check(json: String, error: String) = {
    JsonParser.fromJson(json) must throwA(new IllegalArgumentException("requirement failed: " + error))
  }
  
}