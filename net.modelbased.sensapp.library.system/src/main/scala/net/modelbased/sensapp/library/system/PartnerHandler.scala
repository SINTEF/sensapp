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
 * Module: net.modelbased.sensapp.library.system
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
package net.modelbased.sensapp.library.system

import akka.actor.ActorSystem

/**
 * Mechanism to handle external partnerships
 * @author Sebastien Mosser
 */
trait PartnerHandler {
  implicit val actorSystem: akka.actor.ActorSystem
  
  type Partner = (String, Int)
  /**
   * return the partner (server, port) associated to a given key (i.e., a service name)
   */
  def apply(key: String): Option[Partner]
}

/**
 * This default partner handler consider that everything is deployed on localhost:8080
 */
trait Monolith extends PartnerHandler {
  val port: Int = 8080
  def apply(key: String) = Some(("localhost", port))
}

/**
 * This partner handler uses a Map to declare partnerships
 */
trait DistributedPartners extends PartnerHandler {
  protected val _partners: Map[String, Partner]
  private[this] val default: Partner =  ("localhost", 8080)
  
  def apply(key: String): Option[Partner]  = {
    Some(_partners.getOrElse(key, default))
  }
}

import cc.spray.typeconversion.SprayJsonSupport

trait TopologyFileBasedDistribution extends DistributedPartners with SprayJsonSupport  {
  
  override lazy val _partners = load()
  
  override def apply(key: String) = _partners.get(key)
  
  /**
   * Topology JSON data structure
   */
  protected val topoUrl: java.net.URL = getClass.getClassLoader.getResource("topology.json")
  
  //import cc.spray._
  import cc.spray.json._
  import cc.spray.json.DefaultJsonProtocol._
  import scala.io.Source 

  case class Node(name: String, server: String, port: Int)
  case class Binding(service: String, node: String)
  case class Topology(nodes: List[Node], bindings: List[Binding])
  
  implicit def nodeFormat = jsonFormat(Node, "name", "srv", "port")
  implicit def bindingFormat = jsonFormat(Binding, "service", "node")
  implicit def topologyFormat = jsonFormat(Topology, "nodes", "deployment")
  
  private[this] def load(): Map[String, Partner] = {
    val topology = Source.fromURL(topoUrl).mkString.asJson.convertTo[Topology]
    val nodes = topology.nodes map { n => n.name -> (n.server, n.port) }
    val nodeMap = nodes.toMap
    val bindings = topology.bindings map { b =>
      val partner = nodeMap(b.node)
      b.service -> (partner._1, partner._2) 
    }
    bindings.toMap
  }
}


