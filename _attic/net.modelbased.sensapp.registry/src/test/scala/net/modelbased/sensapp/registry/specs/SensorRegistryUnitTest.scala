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
package net.modelbased.sensapp.registry.specs
import org.specs2.mutable._
import net.modelbased.sensapp.restful.Helpers._
import net.modelbased.sensapp.restful.JettyEnvironment
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods._
import net.modelbased.sensapp.registry.datamodel.{Sensor, SensorRegistry}

class SensorRegistryUnitTest extends SpecificationWithJUnit with JettyEnvironment {

  "SensorRegistryService Specification Unit".title
  
  private val url = "http://localhost:" + serverPort + "/sensapp-registry/sensors/"
  private val client = new DefaultHttpClient()
  
  step(jettyStart)
   
  "GET: A SensorRegistryService" should {
    "returns a NotFound (404) status for unregistered sensor" in new FilledEnvironment {
      val response = client.execute(new HttpGet(url + unregistered.id))
      release(response)
      getStatusCode(response) must_== 404
    }
    "returns JSON content" in new FilledEnvironment{
      val response = client.execute(new HttpGet(url + s1.id))
      release(response)
      getContentType(response) must contain("application/json")
    }
    "returns the registered sensor" in new FilledEnvironment {
      val response = client.execute(new HttpGet(url + s1.id))
      val data = getBodyContent(response)
      release(response)
      (new SensorRegistry()).fromJSON(data) must_== s1
    }
  }
  
  "PUT: A SensorRegistryService" should {
    "Return a 200 status code after successful execution" in new FilledEnvironment {
      val put = new HttpPut(url+s1.id)
      val s1Prime = Sensor(s1.id, Some(s1.nickname.get+" [Updated]"))
      val response = putData(client, put, List(("descriptor", (new SensorRegistry()).toJSON(s1))))
      release(response)
      getStatusCode(response) must_== 200
    }
    "Update sensor description in the registry" in new FilledEnvironment {
      val put = new HttpPut(url+s1.id)
      val s1Prime = Sensor(s1.id, Some(s1.nickname.get+" [Updated]"))
      val response = putData(client, put, List(("descriptor", (new SensorRegistry()).toJSON(s1Prime))))
      release(response)
      val registry = new SensorRegistry()
      registry.pull(registry.identify(s1)) must beSome(s1Prime)
    }
    "be idempotent" in new FilledEnvironment {
      val put = new HttpPut(url+s1.id)
      val registry = new SensorRegistry()
      val size = registry.size
      val s1Updated = Sensor(s1.id, Some(s1.nickname.get+" [Updated]"))
      val response = putData(client, put, List(("descriptor", registry.toJSON(s1Updated))))
      release(response)
      val responsePrime = putData(client, put, List(("descriptor", registry.toJSON(s1Updated))))
      release(responsePrime)
      registry.size must_== size // FIXME: should be better ... (and assumes test isolation)
    }
    "returns a NotFound (404) status for unregistered sensor" in new FilledEnvironment {
      val put = new HttpPut(url+unregistered.id)
      val response = putData(client, put, List(("descriptor", (new SensorRegistry()).toJSON(unregistered))))
      release(response)
      getStatusCode(response) must_== 404
    }
    "reject bad input with 500 status code" in new FilledEnvironment {
      val put = new HttpPut(url+unregistered.id)
      val response = putData(client, put, List(("descriptor", "bad data")))
      release(response)
      getStatusCode(response) must_== 500
    }
    "Return a conflict (409) when used with a descriptor that does not match the url" in new FilledEnvironment {
      val put = new HttpPut(url+s1.id)
      val response = putData(client, put, List(("descriptor", (new SensorRegistry()).toJSON(s2))))
      release(response)
      getStatusCode(response) must_== 409
    }
  } 
  
  "DELETE: A SensorRegistryService" should {
    "Return a 200 status code after successful execution" in new FilledEnvironment {
      val delete = new HttpDelete(url+s1.id)
      val response = client.execute(delete)
      release(response)
      getStatusCode(response) must_== 200
    }
    "Delete a description from the registry" in new FilledEnvironment {
      val delete = new HttpDelete(url+s1.id)
      val response = client.execute(delete)
      release(response)
      val registry = new SensorRegistry()
      registry.pull(registry.identify(s1)) must beNone
    }
    "returns a NotFound (404) status for unregistered sensor" in new FilledEnvironment {
      val delete = new HttpDelete(url+unregistered.id)
      val response = client.execute(delete)
      release(response)
      getStatusCode(response) must_== 404
    }
  }
  
  step(jettyStop)
}