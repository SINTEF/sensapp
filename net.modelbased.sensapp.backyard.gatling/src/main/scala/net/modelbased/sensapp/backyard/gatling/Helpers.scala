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
 * Module: net.modelbased.sensapp.backyard.gatling.ws
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
package net.modelbased.sensapp.backyard.gatling

object RandomData {
  private[this] val bag = new scala.util.Random	
  def apply(sensorId: String, stamp: Long): String = {
    val data = "{\"e\":[{\"n\":\""+ sensorId + "\", \"u\": \"m\", \"v\": " + bag.nextFloat() +",\"t\": "+ stamp +"}]}"
    data
  } 
}

object RandomBigData {
  private[this] val bag = new scala.util.Random
  def apply(sensorId: String, stamp: Long, size: Int): String = {
    var data = "{\"e\":["
    var i = 0
    for(i <- 0 to size){
      if(i == size)
        data += "{\"n\":\""+ sensorId + "\", \"u\": \"m\", \"v\": " + bag.nextFloat() +",\"t\": "+ stamp +"}]}"
      else
        data += "{\"n\":\""+ sensorId + "\", \"u\": \"m\", \"v\": " + bag.nextFloat() +",\"t\": "+ stamp +"}, "
    }
    data
  }
}

object RandomSensor {
  private[this] var counter = 0
  def apply(prefix: String = "gatling-gen"): String = {
    counter += 1
    val name = prefix+ "/" + counter
    name
  }
}