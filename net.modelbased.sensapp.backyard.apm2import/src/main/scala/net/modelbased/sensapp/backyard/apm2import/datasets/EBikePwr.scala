package net.modelbased.sensapp.backyard.apm2import.datasets

/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.backyard.apm2import
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
/**
 * Created by IntelliJ IDEA.
 * User: franck
 * Date: 01/06/12
 * Time: 07:44
 * To change this template use File | Settings | File Templates.
 */

import net.modelbased.sensapp.backyard.apm2import._

object EBikePwr {

  val pwrlog_files = List("/ebike-20120609-020533.csv", "/ebike-20120609-040457.csv", "/ebike-20120610-011133.csv", "/ebike-20120610-123806.csv")

  val out_folder = "../net.modelbased.sensapp.data.samples/CyclingData/EBikePwr/"

  val altitude_offset = 0
  val ground_altitude = 0

  def main(args : Array[String]) {


    pwrlog_files.foreach{pwrlog_file =>

      var name = pwrlog_file.replaceAll(".csv", "").replaceAll("/", "")

      var pwrdata = EBikeDataParser.parseEBikeLog(pwrlog_file)
      //pwrdata = EBikeDataParser.chopDataSet(pwrdata, 50, 2050)
      EBikeDataParser.writeCSVLog(out_folder + name + "_power.csv", pwrdata)

      var pwrdata1hz = EBikeDataParser.extract1HzData(pwrdata)
      EBikeDataParser.writeCSVLog(out_folder + name + "_power_1hz.csv", pwrdata1hz)

      val basetime = pwrdata1hz.head.time / 1000
      EBikeDataParser.setRelativeTime(pwrdata1hz)

      EBikeDataParser.writeIndividualSenML(out_folder + name + "_1hz", pwrdata1hz, name , basetime)
    }
  }
}