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

import net.modelbased.sensapp.library.senml.{IANA, MeasurementOrParameter}

object MoPComplianceChecker {
  
  def apply(mop: MeasurementOrParameter) { new MoPComplianceChecker().validate(mop) }
  
  val UNKNWOWN_UNIT = "If provided, 'units' must be defined as a IANA unit code"
    
  val AMBIGUOUS_VALUE_PROVIDED = "A value ('sv', 'v', 'bv' or 's') must be provided"
}

class MoPComplianceChecker extends ComplianceChecker[MeasurementOrParameter] {
  
  import MoPComplianceChecker._
  
  override lazy val checks = Map( 
      isKnownUnit -> UNKNWOWN_UNIT,
      existsValue -> AMBIGUOUS_VALUE_PROVIDED
    )
  
  /**
   * Reject an unknown unit (See Unit.scala)
   * Source: "Optional [...]. Acceptable values are specified in Section 10.1"
   * url: http://tools.ietf.org/html/draft-jennings-senml-08#section-4 (page 5)
   */  
  private[this] val isKnownUnit: CheckerFunction = _.units match {
      case None => true
      case Some(code) => IANA(code) != None
    }
  
  
  private[this] val existsValue: CheckerFunction = { mop => 
    mop.valueSum match {
      case None => valueExclusivity(mop)
	  case Some(_) => ((mop.value == None) && (mop.stringValue == None) && (mop.booleanValue == None)) || valueExclusivity(mop)
    }
  }
  
  
  private[this] def valueExclusivity(mOp: MeasurementOrParameter) = {
    (    ((mOp.value != None)        && ((mOp.stringValue == None) && (mOp.booleanValue == None)))
      || ((mOp.stringValue != None)  && ((mOp.value == None)       && (mOp.booleanValue == None)))
      || ((mOp.booleanValue != None) && ((mOp.value == None)       && (mOp.stringValue == None)))
    )
  }  
}

 