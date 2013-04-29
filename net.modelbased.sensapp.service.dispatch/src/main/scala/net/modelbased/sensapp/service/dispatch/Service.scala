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
package net.modelbased.sensapp.service.dispatch


import cc.spray._
import cc.spray.typeconversion.SprayJsonSupport
import net.modelbased.sensapp.library.senml.{Root => SenMLRoot}
import net.modelbased.sensapp.library.senml.export.{JsonProtocol => SenMLProtocol}
import net.modelbased.sensapp.library.system.{Service => SensAppService} 

trait Service extends SensAppService {
  
  import SenMLProtocol._
  
  override lazy val partnerName = "dispatch"
  override lazy val partnersNames = List("database.raw", "registry", "notifier")
    
  val service = {
    path("dispatch") {
      detach {
        put { 
          content(as[SenMLRoot]) { data => context =>
            val handled = data.dispatch.par map {
              case (target, data) => {
                try {
                  Dispatch(partners, target, data.measurementsOrParameters.get)
                  None
                } catch { case e => { actorSystem.log.info(e.toString); Some(target) } }
              }
            }
            context complete handled.filter{ _.isDefined }.toList
          }
        }
      } ~ cors("PUT")
    }
  }
}