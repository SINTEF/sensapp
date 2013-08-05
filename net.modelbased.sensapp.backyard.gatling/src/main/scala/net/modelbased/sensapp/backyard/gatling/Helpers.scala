package net.modelbased.sensapp.backyard.gatling

/**
 * Created with IntelliJ IDEA.
 * User: Jonathan
 * Date: 05/08/13
 * Time: 12:44
 */

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
