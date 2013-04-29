/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 * Module: net.modelbased.sensapp
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
  
  val Struct = "([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*)".r
  val inputDir = "/Users/sebastienm/Desktop/cabspottingdata"
  val outputDir = "data"
  
  val content = (new java.io.File(inputDir)).listFiles.filter(_.getName.contains("new_"))
  content.par foreach { file =>
    println("handling [%s]".format(file))
    val cabId = file.getName.split("_")(1).split("\\.")(0)
    var phi, lambda, occupied = List[String]()
    var bT: Long = -1
    val raw = io.Source.fromFile(file).getLines.toArray foreach {
      _ match {
        case Struct(lat, lon, occupation, stamp) => {
          val time = stamp.toDouble.toLong
          if (bT == -1) bT = time
          phi = "{\"t\":%s, \"v\": %s}".format(time-bT, lat) :: phi
          lambda = "{\"t\":%s, \"v\": %s}".format(time-bT, lon) :: lambda
          occupied = "{\"t\":%s, \"bv\": %s}".format(time-bT, (occupation == "1") ) :: occupied
        }
      }
    }
    val senmlPhi = "{\"bn\":\"sf/cab/%s/phi\", \"bt\": %s, \"bu\": \"lat\", \"e\": [%s]}".format(cabId,bT,phi.mkString(","))
    write(cabId, "phi", senmlPhi)
    val senmlLambda = "{\"bn\":\"sf/cab/%s/lambda\", \"bt\": %s, \"bu\": \"lon\", \"e\": [%s]}".format(cabId,bT,lambda.mkString(","))
    write(cabId, "lambda", senmlLambda)
    val senmlOcc = "{\"bn\":\"sf/cab/%s/occupied\", \"bt\": %s, \"e\": [%s]}".format(cabId,bT,occupied.mkString(","))
    write(cabId, "occupied", senmlOcc)
  }  
  
  private[this] def write(cabId: String, sensor: String, data: String) {
    val outFile = new java.io.File("%s/%s-%s.senml.json".format(outputDir,cabId,sensor))
    val out = new java.io.PrintWriter(outFile)
    try { out.print(data) } finally  { out.close }
    println("  -->> %s: done".format(outFile))
  }
