package net.modelbased.sensapp.backyard.gatling

import com.excilys.ebi.gatling.core.Predef._
import com.excilys.ebi.gatling.http.Predef._
import com.excilys.ebi.gatling.jdbc.Predef._

class FooSimulation extends Simulation {

	def apply = {
		val scn = scenario("SensApp foo")
				.loop( chain
						.exec(http("sensor list").get("http://localhost:8080/databases/raw/sensors"))).times(30)
		List(scn.configure.users(10))
	}
}