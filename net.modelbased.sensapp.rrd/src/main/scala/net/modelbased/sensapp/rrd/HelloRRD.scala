/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.rrd
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
package net.modelbased.sensapp.rrd

import org.rrd4j.{ConsolFun, DsType}
import org.rrd4j.core.{RrdMongoDBBackendFactory, RrdMongoDBBackend, RrdDef}

/**
 * Created by IntelliJ IDEA.
 * User: ffl
 * Date: 14.11.11
 * Time: 15:54
 * To change this template use File | Settings | File Templates.
 */

object HelloRRD {

   def main(args: Array[String]) {

      var rrdPath = "myRRDTest.rrd"

      // Definition of the RRD database: Type of data, consolidation and archives
      var rrdDef : RrdDef = new RrdDef(rrdPath, 300)
      rrdDef.addArchive(ConsolFun.AVERAGE, 0.5, 1, 600) // 1 step, 600 rows
      rrdDef.addArchive(ConsolFun.AVERAGE, 0.5, 6, 700) // 6 steps, 700 rows
      rrdDef.addArchive(ConsolFun.MAX, 0.5, 1, 600);

      var f = new RrdMongoDBBackendFactory()
     f.



   }
}