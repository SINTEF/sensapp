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
 * Module: net.modelbased.sensapp.backyard.gatling
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

//import io.gatling.core.session.Session
//import io.gatling.core.scenario.{scenario, Simulation}
import io.gatling.core.Predef._
import io.gatling.http.Predef._
import bootstrap._
import io.gatling.core.scenario.RampInjection

/**
 * Created with IntelliJ IDEA.
 * User: Jonathan
 * Date: 05/08/13
 * Time: 12:48
 */
class SensorBigPushSimulation extends Simulation {

  val numberOfUsers: Int = 5
  val timeframe: Int = 10
  val numberOfData: Int = 5
  val maxDelayBetweenPush: Int = 400

  def apply = {
    //ramp(numberOfUsers);
    //rampRate(timeframe);
    setUp(sensorPush.inject(RampInjection(numberOfUsers, timeframe)))
    /*List(sensorPush)*/
  }

  val headers = Map("Content-Type" -> "application/json", "Accept" -> "text/plain,application/json")

  val sensorPush =
    scenario("Sensor pushing Data")
      .exec { (session: Session) => // Preparing the session
      session.set("sensorId", RandomSensor())
        .set("stamp", (System.currentTimeMillis / 1000))
    }
      .exec{   // 0. Is SensApp alive?
      http("Is SensApp alive?")
        .get("http://"+Target.serverName+"/databases/raw/sensors")
        .check(status is 200)
    }.pause(100, 200/*, MILLISECONDS*/)
      .exec {  // 1. Creating the database
      http("Creating the database")
        .post("http://"+Target.serverName+"/databases/raw/sensors")
        .headers(headers)
        .body(StringBody("{\"sensor\": \"${sensorId}\", \"baseTime\": ${stamp}, \"schema\": \"Numerical\"}"))
    }.pause(100, 200/*, MILLISECONDS*/)
      .repeat(numberOfData){ // Pushing data
      exec { session: Session =>
        session.set("data", RandomBigData(session("sensorId").as[String],
          session("stamp").as[Long], 50))
      }.exec {
        http("Pushing random data")
          .put("http://"+Target.serverName+"/databases/raw/data/${sensorId}")
          .headers(headers).body(StringBody("${data}"))
      }.exec { (session: Session) =>
        session.set("stamp", session("stamp").as[Long] + 1)
      }.pause(100, maxDelayBetweenPush/*, MILLISECONDS*/)
    }//.times(numberOfData)
      .exec { // 3. Eventually deleting the database
      http("Deleting the database")
        .delete("http://"+Target.serverName+"/databases/raw/sensors/${sensorId}")
    }

  setUp(sensorPush.inject(RampInjection(numberOfUsers, timeframe)))
}
