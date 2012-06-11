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

object EBike1 {

  val log_file = "/EBike1.log"
  val pwrlog_file = "/EBike1-Power.csv"
  val out_folder = "../net.modelbased.sensapp.data.samples/CyclingData/EBike1/"
  val name = "EBike1"

  val altitude_offset = 0
  val ground_altitude = 0

  def main(args : Array[String]) {

    var data = APMDataParser.parseAPMLog(log_file)
    data = APMDataParser.chopDataSet(data, 2500, 8500)
    APMDataParser.fixAltitude(data, altitude_offset)
    //APMDataParser.fixAltitude(data, -ground_altitude)
    APMDataParser.fix10HzTimeIncrements(data)
    APMDataParser.setRelativeTime(data)
    APMDataParser.printStats(data)

    APMDataParser.writeAPMLog(out_folder + "raw/" + name + ".log", data)
    APMDataParser.writeCSVLog(out_folder + "raw/" + name + ".csv", data)
    APMDataParser.writeSRTFile(out_folder + name + ".srt", data, 5500, 100)

    val data1hz = APMDataParser.extract1HzData(data)

    APMDataParser.writeCSVLog(out_folder + "raw/" + name + "_1hz.csv", data1hz)
    APMDataParser.writeSRTFile(out_folder + name + "_1hz.srt", data1hz, 5500 , 1000)
    APMDataParser.writeSenML(out_folder + "raw/" + name + "_1hz.json", data1hz, name , 0)

    APMDataParser.writeIndividualSenML(out_folder + "data/" + name + "_1hz", data1hz, name , 0);


    var pwrdata = EBikeDataParser.parseEBikeLog(pwrlog_file)
    pwrdata = EBikeDataParser.chopDataSet(pwrdata, 50, 2050)
    EBikeDataParser.writeCSVLog(out_folder + "raw/" + name + "_power.csv", pwrdata)

    var pwrdata1hz = EBikeDataParser.extract1HzData(pwrdata)
    EBikeDataParser.writeCSVLog(out_folder + "raw/" + name + "_power_1hz.csv", pwrdata1hz)

    val basetime = pwrdata1hz.head.time / 1000
    EBikeDataParser.setRelativeTime(pwrdata1hz)

    EBikeDataParser.writeIndividualSenML(out_folder + "data/" + name + "_1hz", pwrdata1hz, name , basetime)


  }


}