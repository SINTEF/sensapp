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
package net.modelbased.sensapp.library.senml.spec


object Standard{
  val VERSION_NUMBER = 1 
  val NAME_VALIDATOR = "[a-zA-Z0-9][a-zA-Z0-9-:._/\\[\\]]+"
}

/*

object Standard{
  
  val VERSION_NUMBER = 1
  
  val NAME_VALIDATOR = "[a-zA-Z0-9][a-zA-Z0-9-:._/\\[\\]]+"
  
  object errors {
    val VERSION_MUST_BE_POSITIVE = "If provided, 'version' MUST be a positive integer"
    val UNSUPPORTED_VERSION = "If provided, 'version' MUST be >= " + Standard.VERSION_NUMBER
    val UNKNOWN_BASE_UNIT = "If provided, 'baseUnits' must be defined as a IANA unit code"
    //val NO_UNITS_DEFINED = "As 'baseUnits' is not provided, all measurements must provide an 'unit'"
    //val EMPTY_MEASUREMENTS = "The 'measuddrementsOrParameters' entry cannot be empty"
    val EMPTY_NAME = "As 'baseName' is not provided, all measurements must provides a 'name'"
    val UNKNWOWN_UNIT = "If provided, 'units' must be defined as a IANA unit code"
    val INVALID_NAME = "'baseName'+'name' must match " + NAME_VALIDATOR
    val AMBIGUOUS_VALUE_PROVIDED = "A value ('sv', 'v', 'bv' or 's') must be provided"
  }
  
  object checkers {
   
        
    def allNamesValid(root: Root) = {
      val bN = root.baseName.getOrElse("")
      root.measurementsOrParameters match {
        case None => true
        case Some(lst) => lst.par forall { mOp => (bN + mOp.name.getOrElse("")).matches(NAME_VALIDATOR) }
      }
    }
    

    
  }
}*/