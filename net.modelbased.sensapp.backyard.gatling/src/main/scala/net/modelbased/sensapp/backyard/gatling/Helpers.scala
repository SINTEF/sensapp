package net.modelbased.sensapp.backyard.gatling

import com.excilys.ebi.gatling.core.Predef._
import com.excilys.ebi.gatling.http.Predef._
import com.excilys.ebi.gatling.jdbc.Predef._

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