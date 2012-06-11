package net.modelbased.sensapp.backyard.apm2import

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
import java.lang.{Double, Long}
import collection.mutable.ListBuffer
import io.Source
import java.io.{PrintWriter, File}
import java.text.{DecimalFormatSymbols, DecimalFormat}
import java.util.Locale
import java.util.concurrent.TimeUnit

import net.modelbased.sensapp.library.senml._
import cc.spray.json.DefaultJsonProtocol._
import export.JsonParser
import net.modelbased.sensapp.library.senml.export.JsonProtocol._
import cc.spray.json.DefaultJsonProtocol
import util.parsing.json.{JSONFormat, JSON}
import cc.spray.json._

object EBikeDataParser {

  def parseEBikeLog(log_file : String) : List[EBikeData] = {
    val result = new ListBuffer[EBikeData]()

    val log = if(new File(log_file).exists()) Source.fromFile(log_file)
    else  Source.fromURL(getClass.getResource(log_file))

    val lines = log.getLines()

    var dataline : EBikeData = null

    // Assuming that GPS and ATT are present at the same frequency.
    // GPS line followed by an ATT line.
    lines.foreach{ line =>
      if (!line.startsWith("time")) {
        dataline = new EBikeData(line, dataline)
        result += dataline
      }

    }
    return result.toList
  }

    def writeCSVLog(log_file : String, data : List[EBikeData]) {
    val sb = new StringBuilder()
    val df = new DecimalFormat("0", new DecimalFormatSymbols(Locale.US));
    val df2 = new DecimalFormat("0.00", new DecimalFormatSymbols(Locale.US));
    val df4 = new DecimalFormat("0.0000", new DecimalFormatSymbols(Locale.US));
    sb append  "time, voltage, current, dt, dah, dwh, useah, usewh, chgah, chgwh, t1, t2\n"
    data.foreach{ d =>
      sb append  "" + d.time + ", " + df2.format (d.volt) + ", " + df2.format (d.curr) + ", " + d.dt + ", "
      sb append  df4.format (d.dah) + ", " + df4.format (d.dwh) + ", "
      sb append  df2.format (d.useah) + ", " + df2.format (d.usewh) + ", "
      sb append  df2.format (d.genah) + ", " + df2.format (d.genwh) + ", "
      sb append  df.format (d.t1) + ", " + df.format (d.t2) + "\n"
    }
    val out = new PrintWriter(new File(log_file))
    try{ out.print( sb.toString() ) }
    finally{ out.close }
  }

  def writeIndividualSenML(file : String, data : List[EBikeData], name : String, base_time : Long) {

    val sensors = Map(  "voltage" -> ListBuffer[MeasurementOrParameter](),
                        "current" -> ListBuffer[MeasurementOrParameter](),
                        "ah" -> ListBuffer[MeasurementOrParameter](),
                        "wh" -> ListBuffer[MeasurementOrParameter](),
                        "ctrl_temp" -> ListBuffer[MeasurementOrParameter](),
                        "air_temp" -> ListBuffer[MeasurementOrParameter]())

    val units = Map(    "voltage" -> Some(IANA.volt.symbol),
                        "current" -> Some(IANA.ampere.symbol),
                        "ah" -> Some(IANA.count.symbol),
                        "wh" -> Some(IANA.count.symbol),
                        "ctrl_temp" -> Some(IANA.degC.symbol),
                        "air_temp" -> Some(IANA.degC.symbol))

    data.foreach{ d =>
      sensors("voltage") += MeasurementOrParameter(None, None, Some(d.volt),None, None, None, Some(d.time/1000), None)
      sensors("current") += MeasurementOrParameter(None, None, Some(d.curr),None, None, None, Some(d.time/1000), None)
      sensors("ah") += MeasurementOrParameter(None, None, None ,None, None, Some(d.dah), Some(d.time/1000), None)
      sensors("wh") += MeasurementOrParameter(None, None, None ,None, None, Some(d.dwh), Some(d.time/1000), None)
      sensors("ctrl_temp") += MeasurementOrParameter(None, None, Some(d.t1),None, None, None, Some(d.time/1000), None)
      sensors("air_temp") += MeasurementOrParameter(None, None, Some(d.t2),None, None, None, Some(d.time/1000), None)
    }

    sensors.foreach{ case (k,v) =>
     val out = new PrintWriter(new File(file + "_" + k + ".senml.json"))
      try {
        v.toList.foreach{ d =>
          val root = Root(Some(name + "/" + k), Some(base_time), units.getOrElse(k, None), None, Some(Seq(d)))
          out.print( JsonParser.toJson(root) + "\n" )
        }
      }
      finally{ out.close }
    }



  }

  def chopDataSet(data : List[EBikeData], start : java.lang.Integer, end : java.lang.Integer) : List[EBikeData] = {
    val result = new ListBuffer[EBikeData]()
    var count = 0
    data.foreach{ d =>
      if (count >= start && count <= end) result.append(d)
      count+=1
    }
    result.toList
  }

  def extract1HzData(data : List[EBikeData]) : List[EBikeData] = {
    val result = new ListBuffer[EBikeData]()
    val start = data.head.time
    var prev : EBikeData = null
    var sec = -1
    data.foreach{ d =>
      if ( (d.time - start) / 1000 > sec ) {
        prev = new EBikeData(d.toString(), prev)
        result += prev
        sec = ((d.time - start) / 1000).toInt
      }
    }
    return result.toList
  }

  def setRelativeTime(data : List[EBikeData]) {
    val offset = data.head.time
    data.foreach{ d => d.time -= offset }
  }



  /*
  def writeSRTFile(srt_file : String, data : List[APMData], offset : Long, delay : Long) {
    val sb = new StringBuilder()
    var start = data.head.gps.time + offset
    var num = 0
    val df = new DecimalFormat("###", new DecimalFormatSymbols(Locale.US));
    val df3 = new DecimalFormat("000", new DecimalFormatSymbols(Locale.US));
    data.foreach{ d =>
      if (d.gps.time >= start) {
        sb append "" + num + "\n"
        val t1 = new TimeMs(d.gps.time - start)
        val t2 = new TimeMs(d.gps.time - start + delay)
        sb append t1.timestampHMS() + " --> " + t2.timestampHMS() + "\n"
        sb append "Altitude: " + df.format(d.gps.alt) + "m"
        if (d.gps.fix) sb append "  Speed: " + df.format(d.gps.speed * 3.6) + "km/h\n"
        else sb append "  Speed: ??km/h\n"
        sb append "Heading: " + df3.format(d.imu.yaw/100) + "  Bank: " + df.format(d.imu.roll/100) + "°  Pitch: " + df.format(d.imu.pitch/100) + "°\n"
        sb append "\n"
        num+=1
      }
    }
    val out = new PrintWriter(new File(srt_file))
    try{ out.print( sb.toString() ) }
    finally{ out.close }
  }
  */


}



// time, current, voltage, t1, t2
// 1338740120440, -0.2, 53.2, 19, 19
class EBikeData(line : String, previous : EBikeData) {

  val data = line.trim.split(",")

  if (data.size != 5) {
    println("ERROR: Malformed EBike data. Should have 5 values on the line (found " + data.size + ")")
  }

  var time = Long.parseLong(data(0).trim)         // Time - ms
  var curr = Double.parseDouble(data(1).trim)       // current (amps)
  var volt = Double.parseDouble(data(2).trim)       // voltage (volts)
  var t1 =   Integer.parseInt(data(3).trim)
  var t2 =   Integer.parseInt(data(4).trim)

  var pwr = curr * volt

  var dt : Long = 0

  var dah : Double = 0
  var dwh : Double = 0

  var genah : Double = 0
  var genwh : Double = 0

  var useah : Double = 0
  var usewh : Double = 0

  if  (previous != null) {
    dt = time - previous.time
    dah = (previous.curr * dt) / 3600 // in mAh
    dwh = (previous.pwr * dt) / 3600  // in mWh
    if (previous.curr > 0) {
      useah = previous.useah + dah;
      usewh = previous.usewh + dwh;
      genah = previous.genah;
      genwh = previous.genwh;
    }
    else {
      genah = previous.genah - dah;
      genwh = previous.genwh - dwh;
      useah = previous.useah;
      usewh = previous.usewh;
    }
  }

  override def  toString() = "" + time + ", " + curr + ", " + volt + ", " + t1 + ", " + t2

}
