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
import java.util.Date
import java.text.SimpleDateFormat

/**
 * Sample application to show how the DSL work
 * @author mosser
 */
object Main extends App with EKlimaDSL {
  
  val now = new Date
  val today = (new SimpleDateFormat("yyyy-MM-dd")) format now
  val start = (new SimpleDateFormat("yyyy-01-01")) format now
  
  
  // Data from Lilleaker station (id: #18980)
  (2000 to 2011).par map { y =>
    18980.between(y+"-01-01", y+"-12-31")   -> "./src/main/resources/"
  }
  18980.between(start, today) -> "./src/main/resources/"
  
  // Data from Blindern station (id: #18700)
  (1970 to 2011).par map { y => 
    18700.between(y + "-01-01", y + "-12-31") -> "./src/main/resources/"
  }
  18700.between(start, today)  -> "./src/main/resources/" 
  
}