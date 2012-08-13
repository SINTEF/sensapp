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
package net.modelbased.sensapp.backyard.apm2import;

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

object APMDataParser {

  def parseAPMLog(log_file : String) : List[APMData] = {
    val result = new ListBuffer[APMData]()

    val log = if(new File(log_file).exists()) Source.fromFile(log_file)
    else  Source.fromURL(getClass.getResource(log_file))

    val lines = log.getLines()

    var gps : GPSData = null
    var imu : IMUData = null
    var apm : APMData = null

    // Assuming that GPS and ATT are present at the same frequency.
    // GPS line followed by an ATT line.
    lines.foreach{ line =>
      if (line.startsWith("GPS")) {
        gps = new GPSData(line)
      }
      else if (line.startsWith("ATT") && gps != null) {
        imu = new IMUData(line)
        apm = new APMData(gps, imu)
        result += apm
      }
      else {
        // nothing
      }
    }
    return result.toList
  }

  def toSenML(data : List[APMData], name : String, base_time : Long) : Root = {
    val buf = ListBuffer[MeasurementOrParameter]()
    data.foreach{ d => d.appendSenML(buf) }
    Root(Some(name + "/"), Some(base_time), None, None, Some(buf.toSeq))
  }

  def writeSenML(file : String, data : List[APMData], name : String, base_time : Long) {
    //val sb = new StringBuilder()
    //data.foreach{ d => d.appendLogLines(sb) }
    val out = new PrintWriter(new File(file))
    try{ out.print( JsonParser.toJson(toSenML(data, name, base_time)) )}
    finally{ out.close }
  }

  def writeIndividualSenML(file : String, data : List[APMData], name : String, base_time : Long) {

    val sensors = Map(  "altitude" -> ListBuffer[MeasurementOrParameter](),
                        "ground_speed" -> ListBuffer[MeasurementOrParameter](),
                        "heading" -> ListBuffer[MeasurementOrParameter](),
                        "pitch" -> ListBuffer[MeasurementOrParameter](),
                        "roll" -> ListBuffer[MeasurementOrParameter](),
                        "gps_alt" -> ListBuffer[MeasurementOrParameter](),
                        "sonar" -> ListBuffer[MeasurementOrParameter](),
                        "crs" -> ListBuffer[MeasurementOrParameter](),
                        "latitude" -> ListBuffer[MeasurementOrParameter](),
                        "longitude" -> ListBuffer[MeasurementOrParameter](),
                        "gpsfix" -> ListBuffer[MeasurementOrParameter](),
                        "gpssats" -> ListBuffer[MeasurementOrParameter]())

    val units = Map(    "altitude" -> Some(IANA.meter.symbol),
                        "ground_speed" -> Some(IANA.velocity.symbol),
                        "heading" -> Some(IANA.radian.symbol),
                        "pitch" -> Some(IANA.radian.symbol),
                        "roll" -> Some(IANA.radian.symbol),
                        "gps_alt" -> Some(IANA.meter.symbol),
                        "sonar" -> Some(IANA.meter.symbol),
                        "crs" -> Some(IANA.meter.symbol),
                        "latitude" -> Some(IANA.lat.symbol),
                        "longitude" -> Some(IANA.lon.symbol),
                        "gpsfix" -> None,
                        "gpssats" -> Some(IANA.count.symbol))

    data.foreach{ d =>
      /*
      sensors("altitude") += MeasurementOrParameter(Some("altitude"), Some(IANA.meter.symbol), Some(d.gps.alt),None, None, None, Some(d.gps.time/1000), None)
      sensors("ground_speed") += MeasurementOrParameter(Some("ground_speed"), Some(IANA.velocity.symbol), Some(d.gps.speed),None, None, None, Some(d.gps.time/1000), None)
      sensors("heading") += MeasurementOrParameter(Some("heading"), Some(IANA.radian.symbol), Some((d.imu.yaw * 0.0174532925)/100),None, None, None, Some(d.gps.time/1000), None)
      sensors("pitch") += MeasurementOrParameter(Some("pitch"), Some(IANA.radian.symbol), Some((d.imu.pitch * 0.0174532925)/100),None, None, None, Some(d.gps.time/1000), None)
      sensors("roll") += MeasurementOrParameter(Some("roll"), Some(IANA.radian.symbol), Some((d.imu.roll * 0.0174532925)/100),None, None, None, Some(d.gps.time/1000), None)
      sensors("gps_alt") += MeasurementOrParameter(Some("gps_alt"), Some(IANA.meter.symbol), Some(d.gps.gps_alt),None, None, None, Some(d.gps.time/1000), None)
      sensors("sonar") += MeasurementOrParameter(Some("sonar"), Some(IANA.meter.symbol), Some(d.gps.sonar),None, None, None, Some(d.gps.time/1000), None)
      sensors("crs") += MeasurementOrParameter(Some("crs"), Some(IANA.meter.symbol), Some(d.gps.crs),None, None, None, Some(d.gps.time/1000), None)
      sensors("latitude") += MeasurementOrParameter(Some("latitude"), Some(IANA.lat.symbol), None, Some(d.gps.latitudeString()), None, None, Some(d.gps.time/1000), None)
      sensors("longitude") += MeasurementOrParameter(Some("longitude"), Some(IANA.lon.symbol), None, Some(d.gps.longitudeString()), None, None, Some(d.gps.time/1000), None)
      sensors("gpsfix") += MeasurementOrParameter(Some("gpsfix"), None, None, None, Some(d.gps.fix), None, Some(d.gps.time/1000), None)
      sensors("gpssats") += MeasurementOrParameter(Some("gpssats"), Some(IANA.count.symbol),  Some(d.gps.sats), None, None, None, Some(d.gps.time/1000), None)
      */
      sensors("altitude") += MeasurementOrParameter(None, None, Some(d.gps.alt),None, None, None, Some(d.gps.time/1000), None)
      sensors("ground_speed") += MeasurementOrParameter(None, None, Some(d.gps.speed),None, None, None, Some(d.gps.time/1000), None)
      sensors("heading") += MeasurementOrParameter(None, None, Some((d.imu.yaw * 0.0174532925)/100),None, None, None, Some(d.gps.time/1000), None)
      sensors("pitch") += MeasurementOrParameter(None, None, Some((d.imu.pitch * 0.0174532925)/100),None, None, None, Some(d.gps.time/1000), None)
      sensors("roll") += MeasurementOrParameter(None, None, Some((d.imu.roll * 0.0174532925)/100),None, None, None, Some(d.gps.time/1000), None)
      sensors("gps_alt") += MeasurementOrParameter(None, None, Some(d.gps.gps_alt),None, None, None, Some(d.gps.time/1000), None)
      sensors("sonar") += MeasurementOrParameter(None, None, Some(d.gps.sonar),None, None, None, Some(d.gps.time/1000), None)
      sensors("crs") += MeasurementOrParameter(None, None, Some(d.gps.crs),None, None, None, Some(d.gps.time/1000), None)
      sensors("latitude") += MeasurementOrParameter(None, None, None, Some(d.gps.latitudeString()), None, None, Some(d.gps.time/1000), None)
      sensors("longitude") += MeasurementOrParameter(None, None, None, Some(d.gps.longitudeString()), None, None, Some(d.gps.time/1000), None)
      sensors("gpsfix") += MeasurementOrParameter(None, None, None, None, Some(d.gps.fix), None, Some(d.gps.time/1000), None)
      sensors("gpssats") += MeasurementOrParameter(None, None, Some(d.gps.sats), None, None, None, Some(d.gps.time/1000), None)
    }

    sensors.foreach{ case (k,v) =>

 /*
      val out = new PrintWriter(new File(file + "_" + k + ".senml.json"))
      try {
        v.toList.foreach{ d =>
          val root = Root(Some(name + "/" + k), Some(base_time), units.getOrElse(k, None), None, Some(Seq(d)))
          out.print( JsonParser.toJson(root) + "\n" )
        }
      }
      finally{ out.close }
      */

      val out = new PrintWriter(new File(file + "_" + k + ".senml.json"))
      try {
        val root = Root(Some(name + "/" + k), Some(base_time), units.getOrElse(k, None), None, Some(v))
          out.print( JsonParser.toJson(root) + "\n" )
      }
      finally{ out.close }


    }

  }

  def chopDataSet(data : List[APMData], start : java.lang.Integer, end : java.lang.Integer) : List[APMData] = {
    val result = new ListBuffer[APMData]()
    var count = 0
    data.foreach{ d =>
      if (count >= start && count <= end) result.append(d)
      count+=1
    }
    result.toList
  }

  def extract1HzData(data : List[APMData]) : List[APMData] = {
    val result = new ListBuffer[APMData]()
    var t = data.head.gps.time
    data.foreach{ d =>
      if (d.gps.time == t) {
        result.append(d)
        t += 1000
      }
    }
    return result.toList
  }

  def fixAltitude(data : List[APMData], offset : Double) {
    data.foreach{ d => d.gps.alt += offset }
  }

  def setRelativeTime(data : List[APMData]) {
    val offset = data.head.gps.time
    data.foreach{ d => d.gps.time -= offset }
  }

  def setBaseTime(data : List[APMData], time : Long) {
    data.foreach{ d => d.gps.time += time }
  }

  def fix10HzTimeIncrements(data : List[APMData]) {
    var t = data.head.gps.time
    data.foreach{ d =>
      d.gps.time = t
      t += 100
    }
  }

  def writeAPMLog(log_file : String, data : List[APMData]) {
    val sb = new StringBuilder()
    data.foreach{ d => d.appendLogLines(sb) }
    val out = new PrintWriter(new File(log_file))
    try{ out.print( sb.toString() ) }
    finally{ out.close }
  }

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

  def writeCSVLog(log_file : String, data : List[APMData]) {
    val sb = new StringBuilder()
    data.head.appendCSVHeader(sb)
    data.foreach{ d => d.appendCSVLine(sb) }
    val out = new PrintWriter(new File(log_file))
    try{ out.print( sb.toString() ) }
    finally{ out.close }
  }

  def getDuration(data : List[APMData]) : String  = {
    val ms = data.last.gps.time - data.head.gps.time
    val h = (ms / 1000) / 3600
    val m = ((ms / 1000) / 60) % 60
    val s = (ms / 1000) % 60
    return "" + h + ":" + m + ":" + s
  }

  def printStats(data : List[APMData]) {
    println ("Number of data points: " + data.size)
    println ("Dataset total duration: " + getDuration(data))
  }

}

class TimeMs(_ms: Long) {

  val ms = _ms

  val hours = { (ms / 1000) / 3600 }
  val minutes = { ((ms / 1000) % 3600) / 60 }
  val seconds = { (ms / 1000) % 60 }
  val milliseconds = { ms % 1000 }

  def timestampHMS() : String = {
    val df = new DecimalFormat("00", new DecimalFormatSymbols(Locale.US));
    val df2 = new DecimalFormat("000", new DecimalFormatSymbols(Locale.US));
    return df.format(hours) + ":" + df.format(minutes) + ":" + df.format(seconds) + "," + df2.format(milliseconds)
  }

}


// Parse lines like:
// GPS: 66210250, 1, 4, 59.9631350, 10.7263600, 0, 130.7500, 208.8200, 0.1400, 170.2500
// GPS: time, fix, sats, latitute, longitude, sonar, alt, gps_alt, speed, CRS
class GPSData(line : String) {

  if (!line.startsWith("GPS:")) {
    println("ERROR: GPS data line should start with GPS:")
  }

  val data = line.substring(4).trim.split(",")

  if (data.size != 10) {
    println("ERROR: Malformed GPS data. Should have 10 values on the line (found " + data.size + ")")
  }

  var time = Long.parseLong(data(0).trim)           // Time of the Week (UTC) in ms
  var fix = data(1).trim.equals("1")                // GPS Fix Status ("1" for true)
  var sats = Integer.parseInt(data(2).trim)         // Number of Locked Satellites
  var lat = data(3).trim                            // GPS Latitude (decimal deg)
  var long = data(4).trim                           // GPS Longitude (decimal deg)
  var sonar = Integer.parseInt(data(5).trim)        // Sonar altitude
  var alt = Double.parseDouble(data(6).trim)        // Filtered GPS and Baro Altitude, MSL (m)
  var gps_alt = Double.parseDouble(data(7).trim)    // GPS Altitude, WGS-84 (m)
  var speed = Double.parseDouble(data(8).trim)      // Ground Speed (m/s)
  var crs =  Double.parseDouble(data(9).trim)       // GPS Course (deg)

  val minf = new DecimalFormat("#.###", new DecimalFormatSymbols(Locale.US));

  def latitudeString() : String = {
    var l = Double.parseDouble(lat)
    val prefix = if (l > 0) "N " else "S "
    l = scala.math.abs(l)
    prefix + l.toInt + "' " + minf.format((l - l.toInt) * 60)
  }

  def longitudeString() : String = {
    var l = Double.parseDouble(long)
    val prefix = if (l > 0) "E " else "W "
    l = scala.math.abs(l)
    prefix + l.toInt + "' " + minf.format((l - l.toInt) * 60)
  }

  def toLogLine() : String = {
    val sb = new StringBuilder()
    appendLogLine(sb)
    sb.toString()
  }

  def appendLogLine(sb : StringBuilder) {
    val df = new DecimalFormat("#.####", new DecimalFormatSymbols(Locale.US));
    //df.setDecimalFormatSymbols(DecimalFormatSymbols.)

    sb append "GPS: " + time + ", "
    if (fix) sb append "1" else sb append "0"
    sb append ", " + sats + ", " + lat + ", " + long + ", " + sonar + ", "
    sb append df.format(alt) + ", " + df.format(gps_alt) + ", "
    sb append df.format(speed) + ", " + df.format(crs) + "\n"
  }
}

// Parse lines like
// ATT: -5658, 3063, 17927
// ATT: roll, pitch, yaw
class IMUData(line : String) {

  if (!line.startsWith("ATT:")) {
    println("ERROR: IMU data line should start with ATT:")
  }

  val data = line.substring(4).trim.split(",")

  if (data.size != 3) {
    println("ERROR: Malformed ATT data. Should have 3 values on the line (found " + data.size + ")")
  }

  var roll = Integer.parseInt(data(0).trim)        // Roll Attiude (phi) - deg
  var pitch = Integer.parseInt(data(1).trim)       // Pitch Attitude (theta) - deg
  var yaw = Integer.parseInt(data(2).trim)         // Yaw Attitude (psi) - deg

   def toLogLine() : String = {
    val sb = new StringBuilder()
    appendLogLine(sb)
    sb.toString()
  }

  def appendLogLine(sb : StringBuilder) {
    sb append "ATT: " + roll + ", " + pitch + ", " + yaw + "\n"
  }
}

class APMData(_gps : GPSData, _imu : IMUData) {

  val gps = _gps
  val imu = _imu

  def toLogLines() : String = {
    val sb = new StringBuilder()
    appendLogLines(sb)
    sb.toString()
  }

  def appendLogLines(sb : StringBuilder) {
    gps.appendLogLine(sb)
    imu.appendLogLine(sb)
  }

  def appendCSVHeader(sb : StringBuilder) {
    sb append "time, fix, sats, latitute, longitude, sonar, alt, gps_alt, speed, CRS, roll, pitch, yaw\n"
  }

  def appendCSVLine(sb : StringBuilder) {
    val df = new DecimalFormat("#.####", new DecimalFormatSymbols(Locale.US));

    sb append "" + gps.time + ", "
    if (gps.fix) sb append "1" else sb append "0"
    sb append ", " + gps.sats + ", " + gps.lat + ", " + gps.long + ", " + gps.sonar + ", "
    sb append df.format(gps.alt) + ", " + df.format(gps.gps_alt) + ", "
    sb append df.format(gps.speed) + ", " + df.format(gps.crs) + ", "
    sb append "" + imu.roll + ", " + imu.pitch + ", " + imu.yaw + "\n"

  }

  def appendSenML(senml : ListBuffer[MeasurementOrParameter]) {
    senml += MeasurementOrParameter(Some("altitude"), Some(IANA.meter.symbol), Some(gps.alt),None, None, None, Some(gps.time/1000), None)
    senml += MeasurementOrParameter(Some("gps_alt"), Some(IANA.meter.symbol), Some(gps.gps_alt),None, None, None, Some(gps.time/1000), None)
    senml += MeasurementOrParameter(Some("sonar"), Some(IANA.meter.symbol), Some(gps.sonar),None, None, None, Some(gps.time/1000), None)
    senml += MeasurementOrParameter(Some("crs"), Some(IANA.meter.symbol), Some(gps.crs),None, None, None, Some(gps.time/1000), None)
    senml += MeasurementOrParameter(Some("ground_speed"), Some(IANA.velocity.symbol), Some(gps.speed),None, None, None, Some(gps.time/1000), None)
    senml += MeasurementOrParameter(Some("heading"), Some(IANA.radian.symbol), Some((imu.yaw * 0.0174532925)/100),None, None, None, Some(gps.time/1000), None)
    senml += MeasurementOrParameter(Some("pitch"), Some(IANA.radian.symbol), Some((imu.pitch * 0.0174532925)/100),None, None, None, Some(gps.time/1000), None)
    senml += MeasurementOrParameter(Some("roll"), Some(IANA.radian.symbol), Some((imu.roll * 0.0174532925)/100),None, None, None, Some(gps.time/1000), None)
    senml += MeasurementOrParameter(Some("latitude"), Some(IANA.lat.symbol), None, Some(gps.latitudeString()), None, None, Some(gps.time/1000), None)
    senml += MeasurementOrParameter(Some("longitude"), Some(IANA.lon.symbol), None, Some(gps.longitudeString()), None, None, Some(gps.time/1000), None)
    senml += MeasurementOrParameter(Some("gpsfix"), None, None, None, Some(gps.fix), None, Some(gps.time/1000), None)
    senml += MeasurementOrParameter(Some("gpssats"), Some(IANA.count.symbol),  Some(gps.sats), None, None, None, Some(gps.time/1000), None)

  }

}