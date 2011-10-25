/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Module: ${project.artifactId}
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
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
package net.modelbased.sensapp.restful.specs

import net.modelbased.sensapp.restful.URIPattern
import org.specs2.mutable._

class URIPatternUnitTest extends SpecificationWithJUnit { 

  import URIPattern._ 
  
  "URIPattern Specification Unit".title
  
  "Structure: an URIPattern" should {
    "Reject the null string as input" in {
      (new URIPattern(null)) must throwAn[IllegalArgumentException]
    }
    "Reject the empty string as input" in {
      (new URIPattern("")) must throwAn[IllegalArgumentException]
    }
    "Reject a schema that does not start with a /" in {
      (new URIPattern("foo/bar")) must throwAn[IllegalArgumentException]
    }
  }
  
  "Behavior: an URIPattern" should {
    "match its schema" in {
      val p = new URIPattern("/prefix/{id:integer}")
      (p matches "/prefix/47") must beTrue
    }
    "rejects irrelevant requests" in {
      val p = new URIPattern("/prefix/{id:integer}")
      (p matches "/prefix/foo") must beFalse
    }
    "extract relevant parameters" in {
      val p = new URIPattern("/prefix/{id:integer}")
      val params = p extract "/prefix/47"
      params("id") must_== "47"
    }
  }
  
  
  "Regular Expression builder: the URIPattern companion" should {
    "build the minimal regular expression" in {
      buildRegexp("/") must_== ".*/"
    }
    "support fixed string" in {
      buildRegexp("/foo") must_== ".*/foo"
    }
    "recognize integer parameters" in {
      buildRegexp("/{foo:integer}") must_== """.*/(\d+)"""
    }
    "recognize string parameters" in {
      buildRegexp("/{foo:string}") must_== """.*/(\w+)"""
    }
    "recognize date parameters" in {
      buildRegexp("/{foo:date}") must_== """.*/(\d\d\d\d-\d\d-\d\d)"""
    }
    "support multiple parameters" in {
      buildRegexp("/{s:string}/{i:integer}") must_== """.*/(\w+)/(\d+)"""
    }
  }
  
  "Parameter extraction: the URIPattern companion" should {
    "accept the empty pattern" in {
      extractParameters("/") must beEmpty
    }
    "accept the absence of parameters" in {
      extractParameters("/foo/bar/geek") must beEmpty
    }
    "identify parameter" in {
      val l = extractParameters("/{foo:integer}") 
      l must contain("foo")
    }
    "identify parameter" in {
      val l = extractParameters("/{foo:integer}/{bar:integer}") 
      l must contain("foo","bar").only.inOrder
    }
  }
  
}