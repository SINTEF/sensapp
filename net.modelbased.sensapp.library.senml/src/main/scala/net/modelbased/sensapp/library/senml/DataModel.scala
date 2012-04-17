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
    val measurementsOrParameters: List[MeasurementOrParameter]
    )  {
  
  /** Checkers required for standard compliance **/
  import Standard.errors._
  import Standard.checkers._
  
  require(providedVersionIsPositiveInteger(this), VERSION_MUST_BE_POSITIVE)
  require(isValidVersion(this), UNSUPPORTED_VERSION)
  require(isKnownBaseUnits(this), UNKNOWN_BASE_UNIT)
  require(allUnitsDefined(this), NO_UNITS_DEFINED)
  require(allUnitsKnown(this), UNKNWOWN_UNIT)
  require(measurementsNotEmpty(this), EMPTY_MEASUREMENTS)
  require(allNamesDefined(this), EMPTY_NAME)
  require(allNamesValid(this), INVALID_NAME)
  require(existsValue(this), AMBIGUOUS_VALUE_PROVIDED)
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
	) 


