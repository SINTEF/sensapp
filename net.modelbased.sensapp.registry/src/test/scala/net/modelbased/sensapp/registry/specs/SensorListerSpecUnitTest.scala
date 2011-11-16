/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.registry
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
package net.modelbased.sensapp.registry.specs

import org.specs2.mutable._
import net.modelbased.sensapp.restful.Helpers._
import net.modelbased.sensapp.restful.JettyEnvironment
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods._
import net.modelbased.sensapp.registry.datamodel.{Sensor, SensorRegistry}
import org.junit._

      
class SensorListerSpecUnitTest extends SpecificationWithJUnit with JettyEnvironment {
 
  "SensorListenerService Specification Unit".title
  private val url = "http://localhost:" + serverPort + "/sensapp-registry/sensors"
  private val client = new DefaultHttpClient()

  step(jettyStart)
  
  "GET: A SensorListenerService" should {
    val get = new HttpGet(url)
    "return a 200 status code" in {
      val response = client.execute(get)
      release(response)
      getStatusCode(response) must_== 200
    }
    "return JSON content" in {
      val response = client.execute(get)
      release(response)
      getContentType(response) must contain("application/json")
    }
    "return the empty list when no sensors are registered" in new EmptyRegistry {
      val response = client.execute(get)
      val data = getBodyContent(response)
      release(response)
      data must_== "[]"
    }
    "return all registered sensors as URLs" in new FilledEnvironment {
      val response = client.execute(get)
      val data = getBodyContent(response)
      release(response)
      // FIXME: does not ensure that it does not contain other data
      (data must /(url+"/"+s1.id)) and (data must /(url+"/"+s2.id)) and (data must /(url+"/"+s3.id)) 
    }
    "be idempotent" in new FilledEnvironment {
      val response = client.execute(get)
      val data = getBodyContent(response)
      release(response)
      val responsePrime = client.execute(get)
      val dataPrime = getBodyContent(responsePrime)
      release(responsePrime)
      data must_== dataPrime // Assumes test isolation
    }
  }  
  
  "POST: A SensorListenerService" should {
    val post = new HttpPost(url)
    "return a 201 status when a sensor is posted" in new FilledEnvironment {
      val response = postData(client, post, List(("descriptor", (new SensorRegistry()).toJSON(unregistered))))
      release(response)
      getStatusCode(response) must_== 201
    }
    "give in the headers the location of the posted sensor" in new FilledEnvironment {
      val response = postData(client, post, List(("descriptor", (new SensorRegistry()).toJSON(unregistered))))
      release(response)
      getHeaderValue(response, "Location") must beSome(url + "/" + unregistered.id)
    }
    "store the described sensor in the registry" in new FilledEnvironment {
      val response = postData(client, post, List(("descriptor", (new SensorRegistry()).toJSON(unregistered))))
      release(response)
      val _registry = new SensorRegistry()
      _registry.pull(_registry.identify(unregistered)) must beSome(unregistered)
    }
    "reject bad input with 500 status code" in {
      val response = postData(client, post, List(("descriptor", "bad data")))
      release(response)
      getStatusCode(response) must_== 500
    }
    "identify a Conflict (409) when registering an existing sensor" in new FilledEnvironment {
      val response = postData(client, post, List(("descriptor", (new SensorRegistry()).toJSON(s1))))
      release(response)
      getStatusCode(response) must_== 409
    }
  }
  
  step(jettyStop)
}
