package net.modelbased.sensapp.backyard.gatling

import com.excilys.ebi.gatling.core.Predef._
import com.excilys.ebi.gatling.http.Predef._
import com.excilys.ebi.gatling.jdbc.Predef._


class SensorPushSimulation extends Simulation {
  
  val numberOfUsers: Int = 1000
  val timeframe: Int = 10
  val numberOfData: Int = 200
  val maxDelayBetweenPush: Int = 400
  
  def apply = { List(sensorPush.configure.users(numberOfUsers).ramp(timeframe)) }
  
  val headers = Map("Content-Type" -> "application/json", "Accept" -> "text/plain,application/json")
  
  val sensorPush = scenario("Sensor pushing Data")
  					// Internal stuff: storing timestamp in the session
  					.exec((session: Session) =>
  					  				session.setAttribute("sensorId", RandomSensor())
  					  				       .setAttribute("stamp", (System.currentTimeMillis / 1000)))
  					// 0. Is SensApp alive?
  					.exec(http("Is SensApp alive?").get("http://"+Target.serverName+"/databases/raw/sensors").check(status is 200))
  					.pause(100, 200, MILLISECONDS)
  					// 1. Creating the database
		  			.exec(http("Creating the database")
		  					.post("http://"+Target.serverName+"/databases/raw/sensors")
		  					.headers(headers)
		  					.body("{\"sensor\": \"${sensorId}\", \"baseTime\": ${stamp}, \"schema\": \"Numerical\"}"))
		  			.pause(100, 200, MILLISECONDS)
		  			// Pushing data
		  			.loop( chain
		  					.exec((session: Session) => 
		  					 	    session.setAttribute("data", RandomData(session.getAttribute("sensorId").asInstanceOf[String], 
		  					 	    										session.getAttribute("stamp").asInstanceOf[Long])))
		  					.exec(http("Pushing random data")
		  							.put("http://"+Target.serverName+"/databases/raw/data/${sensorId}")
		  							.headers(headers)
		  							.body("${data}"))
		  					 .exec((session: Session) => 
		  					 	    session.setAttribute("stamp", session.getAttribute("stamp").asInstanceOf[Long] + 1))
		  					 .pause(100, maxDelayBetweenPush, MILLISECONDS)
		  			).times(numberOfData)
		  			// 3. Eventually deleting the database
		  			.exec(http("Deleting the database")
		  					.delete("http://"+Target.serverName+"/databases/raw/sensors/${sensorId}"))
		  			
  object RandomData {
    private[this] val bag = new scala.util.Random	
    def apply(sensorId: String, stamp: Long): String = {
      val data = "{\"e\":[{\"n\":\""+ sensorId + "\", \"u\": \"m\", \"v\": " + bag.nextFloat() +",\"t\": "+ stamp +"}]}"
      data
    } 
  }

  object RandomSensor {
    private[this] var counter = 0
    def apply(prefix: String = "gatling-gen"): String = {
      counter += 1
      val name = prefix+"/"+ counter
      name
    }
  }
}
