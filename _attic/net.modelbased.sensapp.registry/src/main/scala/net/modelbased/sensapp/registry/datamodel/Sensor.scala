/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 * Module: net.modelbased.sensapp
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
package net.modelbased.sensapp.registry.datamodel

/**
 * Sensor description
 * 
 * @param id the unique identifier associated to this sensor
 * @param nickname an optional description for the sensor
 */
case class Sensor(
    val id: String, 
    val nickname: Option[String])
    
    
    
    
    
	//val quantities: List[PhysicalQuantity])

/*
			 updateRate: 	Option[Int],
			 location: 	Option[Location],
			 description: 	Option[String],
			 keywords: 	Option[List[String]],
			 link:			Option[String]) */

/**
 * Location description
 */
//class Location(name: String, ps: Option[GPSCoordinate])					
//class GPSCoordinate(lattitude: Float, longitude: Float)

/**
 * Physical Quantity description (FIXME)					 
 */
//case class PhysicalQuantity(property: PhysicalProperty, unit: QuantityUnit)
//case class PhysicalProperty(name: String)
//case class QuantityUnit(name: String)
