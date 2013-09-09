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
package net.modelbased.sensapp.service.database.raw.backend
import net.modelbased.sensapp.library.senml._



trait BackendStructure {
  
  protected case class SensorMetaData(val name: String, val timestamp: Long, val schema: RawSchemas.Value) 
  
  abstract sealed protected class SensorData
  protected case class NumericalData(val delta: Long, val data: Double, val unit: String) extends SensorData
  protected case class StringData(val delta: Long, val data: String, val unit: String) extends SensorData
  protected case class BooleanData(val delta: Long, val data: Boolean) extends SensorData
  protected case class SummedData(val delta: Long, val data: Double, val unit: String, val instant: Option[Double]) extends SensorData
  protected case class NumericalStreamChunkData(val delta: Long, val update: Int, val data: List[Option[Float]]) extends SensorData 
  
  
  protected def data2senml(d: SensorData): MeasurementOrParameter = d match {
    case ne: NumericalData => MeasurementOrParameter(None, Some(ne.unit), Some(ne.data), None, None, None, Some(ne.delta), None)
    case se: StringData    => MeasurementOrParameter(None, Some(se.unit), None, Some(se.data), None, None, Some(se.delta), None)
    case be: BooleanData   => MeasurementOrParameter(None, None, None, None, Some(be.data), None, Some(be.delta), None)
    case sume: SummedData  => MeasurementOrParameter(None, Some(sume.unit), sume.instant, None, None, Some(sume.data), Some(sume.delta), None)
    case strm: NumericalStreamChunkData => throw new RuntimeException("Not yet implemented")
  }
  
  protected def buildSenML(metadata: SensorMetaData, data: Seq[SensorData]): Root = {
    val baseName = Some(metadata.name)
    val baseTime = Some(metadata.timestamp)
    val result = if (data.isEmpty) {
      Root(baseName, baseTime, None, None, None)
    } else {
      val mops = data.par map { data2senml(_) }
      Root(baseName, baseTime, None, None, Some(mops.seq))
    }
    result
  }
  
  protected def mop2data(reference: Long, mop: MeasurementOrParameter): SensorData = {
    val delta = mop.time.get - reference
    val unit = mop.units
    mop.data match {
      case DoubleDataValue(d)   => NumericalData(delta, d, unit.get)
      case StringDataValue(s)  => StringData(delta, s, unit.get)
      case BooleanDataValue(b) => BooleanData(delta, b)
      case SumDataValue(d,i)   => SummedData(delta, d, unit.get, i)
    }
  }
  
  // assumed as canonized, i.e., self-contained Mop
  /*protected def mop2data(reference: Long, mops: List[MeasurementOrParameter]): List[SensorData] = {
    val r = mops.par map { mop => 
      val delta = mop.time.get - reference
      val unit = mop.units
      mop.data match {
        case DoubleDataValue(d)   => NumericalData(delta, d, unit.get)
        case StringDataValue(s)  => StringData(delta, s, unit.get)
        case BooleanDataValue(b) => BooleanData(delta, b)
        case SumDataValue(d,i)   => SummedData(delta, d, unit.get, i)
      }
    }
    r.toList
  }*/
}

/**
 * Data entry handled by a Raw database
 */
object RawSchemas extends Enumeration {
  val Numerical = Value("Numerical")
  val String    = Value("String")
  val Boolean   = Value("Boolean")
  val Summed    = Value("Summed")
  val NumericalStreamChunk = Value("NumericalStreamChunk")
}
