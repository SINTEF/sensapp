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
 * Module: net.modelbased.sensapp.library.senml
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
package net.modelbased.sensapp.library.senml.export

import net.modelbased.sensapp.library.senml._
import scala.xml._

object XmlParser {

  def fromXml(elem: Elem): Root = {
    null
  }
  
  def toXml(root: Root): Elem = {
    <senml xmlns="urn:ietf:params:xml:ns:senml" 
    	   bn= { handleOpt(root.baseName) } 
           bu= { handleOpt(root.baseUnits)} 
           bt= { handleOpt(root.baseTime) } 
           ver={ handleOpt(root.version)  }>
      { root.measurementsOrParameters.get map (p => toXml(p)) }
    </senml>
  }
  
  def toXml(mop: MeasurementOrParameter): Elem = {
    <e n={handleOpt(mop.name)} 
       u={handleOpt(mop.units)}
       v={handleOpt(mop.value)}
       sv={handleOpt(mop.stringValue)}
       bv={handleOpt(mop.booleanValue)}
       s={handleOpt(mop.valueSum)}
       t={handleOpt(mop.time)}
       ut={handleOpt(mop.updateTime)}/>
  }
  
  private[this] def handleOpt[T](opt: Option[T]): Option[Text] = opt match {
    case None => None
    case Some(t) => Some(new Text(t.toString))
  }
  
}