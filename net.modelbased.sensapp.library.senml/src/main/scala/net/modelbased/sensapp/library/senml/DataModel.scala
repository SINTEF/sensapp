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
import net.modelbased.sensapp.library.senml.spec._

case class Root (
    val baseName:  Option[String], 
    val baseTime:  Option[Long],
    val baseUnits: Option[String],
    val version:   Option[Int],
    val measurementsOrParameters: Option[Seq[MeasurementOrParameter]]
    )  {
    
  RootComplianceChecker(this) // check compliance with SENML standard
 
  /**
   * Return a canonized version of the SenML message contained in this
   */
  def canonized: Root = { 
    this.measurementsOrParameters match {
      case None => Root(None, None, None, version, None)
      case Some(lst) =>  {
        if (isCanonic) { this } else { Root(None, None, None, version, Some(lst map { mop => mop canonized this }))}
      }
    }
  }
  
  /**
   * a SenML message is canonic if no base* elements are provided
   */
  def isCanonic: Boolean = {this.baseName == None && this.baseTime == None && this.baseUnits == None}
  
  /**
   * returns canonized data classified by each sensor
   * root message **ALWAYS** containes at leat one mop by construction
   */
  def dispatch: Map[String, Root] = {
    val canonized = this.canonized
    canonized.measurementsOrParameters match {
      case None => Map()
      case Some(mops) => {
        val targets = (mops.par map { _.name.get }).toSet.toList // remove duplicates
        val content = targets.par map { target: String =>
          val targetMops = mops.par filter { mop => target == mop.name.get }
          target -> Root(None, None, None, version, Some(targetMops.seq))
        }
        content.seq.toMap
      }
    }
  }
  
}

case class MeasurementOrParameter(
	val name:         Option[String],
	val units:        Option[String],
	val value:        Option[Double],
	val stringValue:  Option[String],
	val booleanValue: Option[Boolean],
	val valueSum:     Option[Double],
	val time:         Option[Long],
	val updateTime:   Option[Int]
	) {
  
  MoPComplianceChecker(this)  // check compliance with SENML standard
  
  def data: DataType = {
    valueSum match {
      case Some(sum) => SumDataValue(sum, value)
      case None if value != None => DoubleDataValue(value.get)
      case None if stringValue != None => StringDataValue(stringValue.get)
      case None if booleanValue != None =>BooleanDataValue(booleanValue.get)
      case _ => throw new IllegalArgumentException("Invalid MeasurementOrParamameter Entry")
    }
  }
  
  def canonized(root: Root): MeasurementOrParameter = {
    val unit = extractUnit(root)
    val name = Some(extractName(root))
    val time = Some(extractTime(root))
    data match {
      case DoubleDataValue(d)   => MeasurementOrParameter(name, unit, Some(d), None, None, None, time, None)
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
  
  def extractUnit(root: Root): Option[String] = data match {
    case BooleanDataValue(_) => None
    case _ => units match {
      case None => Some(IANA(root.baseUnits.get).get.symbol)
      case Some(code) => Some(IANA(code).get.symbol)
    }
  }
  
  def extractTime(root: Root): Long = {
    root.baseTime match {
      case None | Some(0) => time match {
        case None | Some(0) => System.currentTimeMillis / 1000 // no time provided => "roughly now" in the SenML spec
        case Some(time) => time
      }
      case Some(basis) => time match {
        case None => basis
        case Some(time) => basis + time
      }
    }
  }
}

sealed abstract class DataType
case class SumDataValue(val d: Double, val i: Option[Double]) extends DataType
case class DoubleDataValue(val d: Double) extends DataType
case class StringDataValue(val d: String) extends DataType
case class BooleanDataValue(val d: Boolean) extends DataType
//Fixme: stream chunk  
  


