/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.restful
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
package net.modelbased.sensapp.restful

import scala.util.matching.Regex

/**
 * A URI pattern, used to declare the URI used as ResourceHandler trigger
 * 
 * A pattern starts with a "/". It can contain fixed elements (e.g., "/prefix"), or
 * variable parts that are assumed as pass-by-uri parameters (e.g., "/{i:integer})".
 * 
 * Supported parameters type are "integer", "string" and "date".
 * 
 * @param schema the URI schema to be used as a pattern
 */
class URIPattern(val schema: String) {
  require(schema != null)
  require(schema != "")
  require(schema startsWith "/")
  
  /**
   * Check if the given request matched the declared schema
   * @param request the request to be checked
   */
  def matches(request: String): Boolean = {
    (_regexp unapplySeq request) match {
      case Some(_) => true
      case None => false
    }
  }
  
  /**
   * extract the parameters contained in a request according to the declared schema
   */
  def extract(request: String): Map[String, String] = {
    val data = (_regexp unapplySeq request) match {
      case Some(list) => list
      case None => throw new IllegalArgumentException("request does not match")
    }
    var result = Map[String, String]()
    (_parameters zip data) foreach { tuple => result += (tuple._1 -> tuple._2) }
    result
  }
  
  // Internal regular expression used to recognize pattern elements
  private[this] val _regexp: Regex = URIPattern.buildRegexp(schema).r
  // The declared name of the schema parameter (e.g., name for '{name:string}')
  private[this] val _parameters: List[String] = URIPattern.extractParameters(schema)
}

/**
 * Companion object for the URIPattern class
 */
object URIPattern {

  // Regular expression used to identify variable elements
  private[this] final val Variable = """\{(\w+):(integer|string|date)\}""".r
  
  /**
   * Build a regular expression string declaration able to recognize a schema
   * @param schema the schema to be recognized
   * @return a string that model the expected regular expression
   */
  def buildRegexp(schema: String): String = {
    require(schema startsWith "/")
    val parts = ((schema.substring(1,schema.length)) split "/").map{ 
      _ match {
      	case Variable(s, "integer") => """(\d+)"""
      	case Variable(s, "string") => """(\w+)"""
      	case Variable(s, "date") => """(\d\d\d\d-\d\d-\d\d)"""
      	case str => str
      }
    }
    ".*/" + (parts mkString "/")
  }
  
  /**
   * extract the name of the paramneters declared in a schema
   * 
   * @param schema the schema used for extraction
   */
  def extractParameters(schema: String): List[String] = {
    val tmp = schema.split("/") map { 
      _ match {  
        case Variable(s,_) => s 
        case _ => null
      }
    }
    (tmp filter { _ != null }) toList
  }

}
