/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.registry
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
package net.modelbased.sensapp.registry.specs

import org.specs2.mutable._
import net.modelbased.sensapp.registry.datamodel.{Sensor, SensorRegistry}

/**
 * An Empty Registry
 * @author Sebastien Mosser
 */
trait EmptyRegistry extends Before {
  def before { (new SensorRegistry()).dropAll() }
}

/**
 * A registry filled with three sensors
 * 
 * @author Sebastien Mosser
 */
trait FilledEnvironment extends Before {
  
  val s1 = Sensor("s1", Some("1st sensor"))
  val s2 = Sensor("s2", Some("2nd sensor"))
  val s3 = Sensor("s3", Some("3rd sensor"))
  
  val unregistered = Sensor("unregistered", Some("unregistered sensor"))
  val unknown = Sensor("unknown", Some("Unknowm sensor"))
  
  def before {
    val reg = new SensorRegistry()
    reg dropAll()
    reg push s1
    reg push s2
    reg push s3
  }
}