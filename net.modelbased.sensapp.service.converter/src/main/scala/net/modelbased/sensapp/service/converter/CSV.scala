/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: SINTEF ICT <nicolas.ferry@sintef.no>
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

case class CSV(headers : List[String], separator : String) {
	
	val csv : scala.collection.mutable.Map[Long, Array[String]] = scala.collection.mutable.Map()//Timestamp -> list of measurements taken at this time
	
	def push(time : Long, header : String, v : String) {
	  csv.getOrElseUpdate(time, Array.fill[String](headers.size)("-"))(headers.indexOf(header)) = v
	}
	
	override def toString() : String = ((("Timestamp (ms)" :: headers).mkString(separator)) :: (csv.filter{case (time, values) => !values.forall{v => v=="-"}}.toSeq.sortWith(_._1 < _._1).map{case (time,values) => (time.toString :: values.toList).mkString(separator)}).toList).mkString("\n")

}

object Main {
  
  def main(args : Array[String]) {
    val csv = CSV(List("col1", "col2"), ",")
    
    csv.push(123456789, "col1", "v1")
    csv.push(123456789, "col2", "v2")
    csv.push(123456790, "col1", "v3")
    csv.push(123456791, "col2", "v4")
    
    println(csv)
  }
  
}