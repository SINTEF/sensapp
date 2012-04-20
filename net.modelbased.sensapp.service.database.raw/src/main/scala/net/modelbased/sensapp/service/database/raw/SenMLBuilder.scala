/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.service.database.raw
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
package net.modelbased.sensapp.service.database.raw

import net.modelbased.sensapp.library.senml._
import net.modelbased.sensapp.service.database.raw.data._

object SenMLBuilder {

  def build[E <: DataEntry](set: DataSet[E]): Root = {
    val baseName = Some(set.sensor)
    val baseTime = Some(set.baseTime)
    val version = Some(Standard.VERSION_NUMBER)
    val result = if (set.data.isEmpty) {
      Root(baseName, baseTime, None, version, None)
    } else {
      val mops = set.data map { transform(_) }
      Root(baseName, baseTime, None, version, Some(mops))
    }
    result
  }

  
  def transform(e: DataEntry): MeasurementOrParameter = {
    e match {
      case ne: NumericalEntry => MeasurementOrParameter(None, Some(ne.unit), Some(ne.data), None, None, None, Some(ne.delta), None)
      case se: StringEntry    => MeasurementOrParameter(None, Some(se.unit), None, Some(se.data), None, None, Some(se.delta), None)
      case be: BooleanEntry   => MeasurementOrParameter(None, None, None, None, Some(be.data), None, Some(be.delta), None)
      case sume: SummedEntry  => MeasurementOrParameter(None, Some(sume.unit), sume.instant, None, None, Some(sume.data), Some(sume.delta), None)
      case strm: NumericalStreamChunkEntry => throw new RuntimeException("Not yet implemented")
    }
  }
  
}