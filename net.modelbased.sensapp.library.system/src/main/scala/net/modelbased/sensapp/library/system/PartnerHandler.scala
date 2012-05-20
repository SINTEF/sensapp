/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.library.system
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
package net.modelbased.sensapp.library.system

/**
 * Mechanism to handle external partnerships
 * @author Sebastien Mosser
 */
trait PartnerHandler {
  /**
   * return the partner (server, port) associated to a given key (i.e., a service name)
   */
  def apply(key: String): (String, Int)
}

/**
 * This default partner handler consider that everything is deployed on localhost:8080
 */
object Monolith extends PartnerHandler {
  def apply(key: String): (String, Int) = ("localhost", 8080)
}

/**
 * This partner handler uses a Map to declare partnerships
 */
trait DistributedPartners extends PartnerHandler {
  protected val _partners: Map[String, (String, Int)]
  def apply(key: String): (String, Int)  = {
    _partners.getOrElse(key, ("localhost", 8080))
  }
}


