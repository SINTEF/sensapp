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
package net.modelbased.sensapp.library.senml.spec 

import net.modelbased.sensapp.library.senml._

object RootComplianceChecker {
  
  def apply(root: Root) { new RootComplianceChecker().validate(root) }
  
  val VERSION_MUST_BE_POSITIVE = "If provided, 'version' MUST be a positive integer"
  val UNSUPPORTED_VERSION = "If provided, 'version' MUST be >= " + Standard.VERSION_NUMBER
  val UNKNOWN_BASE_UNIT = "If provided, 'baseUnits' must be defined as a IANA unit code"
  val ALL_UNITS_DEFINED = "All MoP must hold an unit, excepting Boolean value (can be set in baseUnit)"
  val EMPTY_NAME = "As 'baseName' is not provided, all measurements must provides a 'name'"
  val UNKNWOWN_UNIT = "If provided, 'units' must be defined as a IANA unit code"
  val AMBIGUOUS_VALUE_PROVIDED = "A value ('sv', 'v', 'bv' or 's') must be provided"
  val EMPTY_MEASUREMENTS = "The 'measurementsOrParameters' entry cannot be empty"
  val INVALID_NAME = "'baseName'+'name' must match " + Standard.NAME_VALIDATOR
}

class RootComplianceChecker extends ComplianceChecker[Root] {
  
  import RootComplianceChecker._
  
  override lazy val checks = Map( 
      isValidVersion       -> UNSUPPORTED_VERSION,
      isKnownBaseUnits     -> UNKNOWN_BASE_UNIT,
      allUnitsDefined      -> ALL_UNITS_DEFINED, 
      measurementsNotEmpty -> EMPTY_MEASUREMENTS,
      allNamesValid        -> INVALID_NAME
    )
  
  /**
     * Reject a message with a version number greater than ours (strong interpretation of SHOULD NOT)
     * Source: "Systems reading one of the objects MUST check for the Version attribute.  If this 
     *          value is a version number larger than the version which the system understands, 
     *          the system SHOULD NOT use this object."
     * url: http://tools.ietf.org/html/draft-jennings-senml-08#section-4 (page 6)
     */
  private[this] val isValidVersion: CheckerFunction = _.version match {
    case None => true
    case Some(v) => v >= Standard.VERSION_NUMBER
  }
 
  /**
   * Reject an unknown base unit (See Unit.scala)
   * Source: "This attribute is optional. Acceptable values are specified in Section 10.1"
   * url: http://tools.ietf.org/html/draft-jennings-senml-08#section-4 (page 4)
   */
  private[this] val isKnownBaseUnits: CheckerFunction = _.baseUnits match {
    case None => true
    case Some(code) => IANA(code) != None
  }
    
  /**
   * Reject a message that does not define a unit for each data, excepting BooleanData that are not considered as eligible for units in this implementation
   * Source: "Optional, if Base Unit is present or if not required for a parameter"
   * url: http://tools.ietf.org/html/draft-jennings-senml-08#section-4 (page 4)
   */
  private[this] val allUnitsDefined: CheckerFunction =  { root => 
    root.baseUnits match {
      case Some(_) => true                        // base unit => true
      case None => {                              // no base unit
        root.measurementsOrParameters match {
          case None => true                       //  AND no data           => true
          case Some(mops) => mops.par forall { mop =>   //  OR AND for all data
            mop.data match {
              case BooleanDataValue(_) => true    //    boolean value => true (no unit)
              case _ => mop.units != None         //    other => unit defined
            } 
          }
        }
      }
    }
  }
  
  /**
   * Rejects a message with an empty list of measures
   * Source: "If present there must be at least one entry in the array."
   * url: http://tools.ietf.org/html/draft-jennings-senml-08#section-4 (page 4)
   */
  private[this] val measurementsNotEmpty: CheckerFunction = { root =>
    root.measurementsOrParameters match {
      case None => true
      case Some(lst) => lst.size > 0
    }
  }
        
  /**
   * Check the validity of used names
   * Source: "The resulting concatenated name MUST consist only of characters out
   *          of the set "A" to "Z", "a" to "z", "0" to "9", "-", ":", ".", or "_"
   *          and it MUST start with a character out of the set "A" to "Z", "a" to
   *          "z", or "0" to "9"."
   * url: http://tools.ietf.org/html/draft-jennings-senml-08#section-4 (page 5)
   */
  private[this] val allNamesValid: CheckerFunction = { root =>
    val bN = root.baseName.getOrElse("")
    root.measurementsOrParameters match {
      case None => true
      case Some(lst) => lst.par forall { mOp => 
        val realName = bN + mOp.name.getOrElse("")
        realName.matches(Standard.NAME_VALIDATOR) }
    }
  }
  
}

