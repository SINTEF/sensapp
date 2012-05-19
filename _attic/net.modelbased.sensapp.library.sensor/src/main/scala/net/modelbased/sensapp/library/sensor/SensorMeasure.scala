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
package net.modelbased.sensapp.library.sensor
import net.modelbased.sensapp.library.senml._

abstract sealed class SensorMeasure
case class NumberData(val name: String, val unit: String, val time: Long, val data: Float) extends SensorMeasure
case class StringData(val name: String, val unit: String, val time: Long, val data: String) extends SensorMeasure
case class BooleanData(val name: String, val unit: String, val time: Long, val data: Boolean) extends SensorMeasure
case class SummedData(val name: String, val unit: String, val time: Long, val data: Float, val instant: Option[Float]) extends SensorMeasure

object SensorMeasureBuilder {

  def canonize(root: Root): List[SensorMeasure] = {
    root.measurementsOrParameters map { canonize(root,_) }
  }
  
  private def canonize(root: Root, mOp: MeasurementOrParameter): SensorMeasure = {
    val name = extractName(root, mOp)
    val unit = extractUnit(root, mOp)
    val time = extractTime(root, mOp)
    extractValue(mOp) match {
      case FloatDataValue(d) => NumberData(name, unit, time, d)
      case StringDataValue(s) => StringData(name, unit, time, s)
      case BooleanDataValue(b) => BooleanData(name, unit, time, b)
      case SumDataValue(d,i) => SummedData(name, unit, time, d, i) 
    }
  }
    
  private def extractName(root: Root, mOp: MeasurementOrParameter): String = {
    root.baseName match {
      case None => mOp.name.get
      case Some(prefix) => prefix + mOp.name.getOrElse("")
    }
  }
  
  private def extractUnit(root: Root, mOp: MeasurementOrParameter): String = {
    mOp.units match {
      case None => IANA(root.baseUnits.get).get.symbol
      case Some(code) => IANA(code).get.symbol
    }
  }
  
  private def extractTime(root: Root, mOp: MeasurementOrParameter): Long = {
    root.baseTime match {
      case None => mOp.time match {
        case None => System.currentTimeMillis / 1000 // no time provided => "roughly now" in the SenML spec
        case Some(time) => time
      }
      case Some(basis) => mOp.time match {
        case None => basis
        case Some(time) => basis + time
      }
    }
  }
  
  private abstract class ValueType
  private case class SumDataValue(val d: Float, val i: Option[Float]) extends ValueType
  private case class FloatDataValue(val d: Float) extends ValueType
  private case class StringDataValue(val d: String) extends ValueType
  private case class BooleanDataValue(val d: Boolean) extends ValueType
  
  private def extractValue(mOp: MeasurementOrParameter): ValueType = {
    mOp.valueSum match {
      case Some(sum) => SumDataValue(sum, mOp.value)
      case None if mOp.value != None => FloatDataValue(mOp.value.get)
      case None if mOp.stringValue != None => StringDataValue(mOp.stringValue.get)
      case None if mOp.booleanValue != None =>BooleanDataValue(mOp.booleanValue.get)
      case _ => throw new IllegalArgumentException("Invalid MeasurementOrParamameter Entry")
    }
  }
  
}