package net.modelbased.sensapp.backyard.gatling

//import io.gatling.core.session.Session
//import io.gatling.core.scenario.{scenario, Simulation}
import io.gatling.core.Predef._
import io.gatling.http.Predef._
import bootstrap._
import io.gatling.core.scenario.RampInjection
import io.gatling.core.structure.ProfiledScenarioBuilder

/**
 * Created with IntelliJ IDEA.
 * User: Jonathan
 * Date: 05/08/13
 * Time: 12:48
 */
class SensorPushSimulation extends Simulation {

  val numberOfUsers: Int = 10
  val timeframe: Int = 10
  val numberOfData: Int = 200
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
        session.set("data", RandomData(session("sensorId").as[String],
          session("stamp").as[Long]))
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
