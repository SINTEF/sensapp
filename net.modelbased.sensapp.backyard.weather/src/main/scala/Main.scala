/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.backyard.weather
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

import net.modelbased.sensapp.backyard.weather.EKlimaDSL

/**
 * Sample application to show how the DSL work
 * @author mosser
 */
object Main extends App with EKlimaDSL {
    
  // Data from Lilleaker station (id: #18980)
  (2000 to 2011).par map { y =>
    18980.between(y+"-01-01", y+"-12-31")   -> "./src/main/resources/"
  }
  19980.between("2012-01-01", "2012-04-24") -> "./src/main/resources/"
  
  // Data from Blindern station (id: #18700)
  (1937 to 2011).par map { y => 
    18700.between(y + "-01-01", y + "-12-31") -> "./src/main/resources/"
  }
  187000.between("2012-01-01", "2012-04-24")  -> "./src/main/resources/"
  
}