/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.service.dispatch
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
  
  override val name = "dispatch"
    
  val service = {
    path("dispatch") {
      detach {
        put { 
          content(as[SenMLRoot]) { data => context =>
            val canonized = data.canonized
            canonized.measurementsOrParameters match {
              case None => context complete "done"
              case Some(mops) => {
                val targets = (mops.par map { _.name.get }).toSet
                val dispatched = targets map { t => 
                  val data = mops.par filter { _.name.get == t }
                  try {
                    Dispatch(partners, t, data.seq)
                    None
                  } catch { case e: Exception => Some(t) }
                }
                context complete dispatched.filter{ _.isDefined }.toList
              }
            }
          }
        }
      }
    }
  }
}