/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
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
package net.modelbased.sensapp.library.senml

case class Root (
    val baseName:  Option[String], 
    val baseTime:  Option[Long],
    val baseUnits: Option[String],
    val version:   Option[Int],
    val measurementsOrParameters: Option[List[MeasurementOrParameter]]
    )  {
  
  /** Checkers required for standard compliance **/
  import Standard.errors._
  import Standard.checkers._
  require(providedVersionIsPositiveInteger(this), VERSION_MUST_BE_POSITIVE)
  require(isValidVersion(this), UNSUPPORTED_VERSION)
  require(isKnownBaseUnits(this), UNKNOWN_BASE_UNIT)
  //require(allUnitsDefined(this), NO_UNITS_DEFINED)
  require(allUnitsKnown(this), UNKNWOWN_UNIT)
  //require(measurementsNotEmpty(this), EMPTY_MEASUREMENTS)
  require(allNamesDefined(this), EMPTY_NAME)
  require(allNamesValid(this), INVALID_NAME)
  require(existsValue(this), AMBIGUOUS_VALUE_PROVIDED) 
  
  def canonized: Root = { 
    this.measurementsOrParameters match {
      case None => Root(None, None, None, version, None)
      case Some(lst) =>  Root(None, None, None, version, Some(lst map { mop => mop canonized this }))
    }
    val mops =  measurementsOrParameters
    Root(None, None, None, version, mops)
  }
}

case class MeasurementOrParameter(
	val name:         Option[String],
	val units:        Option[String],
	val value:        Option[Float],
	val stringValue:  Option[String],
	val booleanValue: Option[Boolean],
	val valueSum:     Option[Float],
	val time:         Option[Long],
	val updateTime:   Option[Int]
	) {
  
  def data: DataType = {
    valueSum match {
      case Some(sum) => SumDataValue(sum, value)
      case None if value != None => FloatDataValue(value.get)
      case None if stringValue != None => StringDataValue(stringValue.get)
      case None if booleanValue != None =>BooleanDataValue(booleanValue.get)
      case _ => throw new IllegalArgumentException("Invalid MeasurementOrParamameter Entry")
    }
  }
  
  def canonized(root: Root): MeasurementOrParameter = {
    val name = Some(extractName(root))
    val unit = Some(extractUnit(root))
    val time = Some(extractTime(root))
    data match {
      case FloatDataValue(d)   => MeasurementOrParameter(name, unit, Some(d), None, None, None, time, None)
      case StringDataValue(s)  => MeasurementOrParameter(name, unit, None, Some(s), None, None, time, None)
      case BooleanDataValue(b) => MeasurementOrParameter(name, unit, None, None, Some(b), None, time, None)
      case SumDataValue(d,i)   => MeasurementOrParameter(name, unit, i, None, None, Some(d), time, None)
    }
  }
  
  def extractName(root: Root): String = {
    root.baseName match {
      case None => name.get
      case Some(prefix) => prefix + name.getOrElse("")
    }
  }
  
  def extractUnit(root: Root): String = {
    units match {
      case None => IANA(root.baseUnits.get).get.symbol
      case Some(code) => IANA(code).get.symbol
    }
  }
  
  def extractTime(root: Root): Long = {
    root.baseTime match {
      case None => time match {
        case None => System.currentTimeMillis / 1000 // no time provided => "roughly now" in the SenML spec
        case Some(time) => time
      }
      case Some(basis) => time match {
        case None => basis
        case Some(time) => basis + time
      }
    }
  }
}

abstract class DataType
case class SumDataValue(val d: Float, val i: Option[Float]) extends DataType
case class FloatDataValue(val d: Float) extends DataType
case class StringDataValue(val d: String) extends DataType
case class BooleanDataValue(val d: Boolean) extends DataType
//Fixme: stream chunk  
  


