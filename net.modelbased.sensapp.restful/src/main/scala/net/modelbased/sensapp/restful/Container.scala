/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Module: ${project.artifactId}
 * Copyright (C) 2011-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
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
package net.modelbased.sensapp.restful

import akka.actor.{Actor, ActorRef}
import akka.http.{Endpoint, RootEndpoint}

/**
 * Model a container used to store resource handlers
 */
trait Container extends Actor with Endpoint{
  
  // type alias used as syntactic sugar
  private type Binding = (Endpoint.Hook, Endpoint.Provider)
  
  /**
   * The resource handler binding that are actually registered in this container
   */
  protected val _registered: List[Binding]
  
  /**
   * bind a resource handler to its trigger pattern
   * @param p the pattern used as trigger
   * @param factory a function that use the requested path to instantiate a new handler
   * 
   * Assuming RH as a concrete ResourceHandler, and p its triggering pattern:
   *   bind(p, req => new RH(p, req))
   * 
   */
  protected def bind(p: URIPattern, factory: String => ResourceHandler): Binding = {
    val hook: Endpoint.Hook = {uri => p matches uri }
    val provider: Endpoint.Provider = { uri => Actor.actorOf(factory(uri)).start }
    (hook, provider)
  }
  
  /**
   * Attach a couple (hook,provider) to the root endpoint 
   * @param h the hook function
   * @param p the provider function
   */
  private def attach(h: Endpoint.Hook, p: Endpoint.Provider) {
    val root: ActorRef = Actor.registry.actorsFor(classOf[RootEndpoint]).head
    root ! Endpoint.Attach(h,p)  
  }
  
  // the dispatcher to be used
  self.dispatcher = Endpoint.Dispatcher
  // the receive method, used to forward request
  def receive = handleHttpRequest 
  
  // overriding the preStart actor method, declared as final to control overriding
  override final def preStart() { _registered foreach { b => attach(b._1, b._2) } }
  
  
}