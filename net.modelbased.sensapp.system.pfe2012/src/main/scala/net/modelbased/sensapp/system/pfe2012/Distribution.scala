/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.system.pfe2012
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
package net.modelbased.sensapp.system.pfe2012

import akka.actor.ActorSystem
import net.modelbased.sensapp.service.database.raw.RawDatabaseService
import net.modelbased.sensapp.service.registry.{ RegistryService, CompositeRegistryService }
import net.modelbased.sensapp.service.dispatch.{ Service => DispatchService }
import net.modelbased.sensapp.service.notifier.{ Service => NotifierService }
import net.modelbased.sensapp.service.converter.{ Service => ConverterService }
import net.modelbased.sensapp.library.system._ 

abstract class DistributedService(override val system: ActorSystem) extends System {
  trait topology { 
    lazy val partners = new TopologyFileBasedDistribution { implicit val actorSystem = system }
    implicit def actorSystem = system 
  }
}


class DatabaseSystem(system: ActorSystem) extends DistributedService(system) {
  def services = List(new RawDatabaseService with topology {})
}

class RegistrySystem(system: ActorSystem) extends DistributedService(system) {
  def services = List(new RegistryService with topology {})
}

class CompositeRegistrySystem(system: ActorSystem) extends DistributedService(system) {
  def services = List(new CompositeRegistryService with topology {})
}

class DispatchSystem(system: ActorSystem) extends DistributedService(system) {
  def services = List(new DispatchService with topology {})
}


class NotifierSystem(system: ActorSystem) extends DistributedService(system) {
  def services = List(new NotifierService with topology {})
}

class ConverterSystem(system: ActorSystem) extends DistributedService(system) {
  def services = List(new ConverterService with topology {})
}

