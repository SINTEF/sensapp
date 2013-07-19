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
 * Module: net.modelbased.sensapp.service.ws
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
package net.modelbased.sensapp.service.ws

import net.modelbased.sensapp.library.senml.{Root, MeasurementOrParameter}
import net.modelbased.sensapp.service.notifier.data.SubscriptionRegistry
import net.modelbased.sensapp.service.notifier.protocols.ProtocolFactory
import net.modelbased.sensapp.service.database.raw.backend.impl.MongoDB
import net.modelbased.sensapp.service.database.raw.backend.Backend


/**
 * Created with IntelliJ IDEA.
 * User: Jonathan
 * Date: 18/07/13
 * Time: 13:56
 */
object WsServerHelper {
  private[this] val _backend: Backend = new MongoDB()
  private[this] val _registry = new SubscriptionRegistry()

  def doOrder(order: String): String = {
    var myOrder = order
    getFunctionName(order) match{
      case "getNotifications" => /*{
        return _registry retrieve List() toString()
      }                            */
      case "registerNotification" =>
      case "getNotification" =>
      case "deleteNotification" =>
      case "updateNotification" =>

      case "dispatch" =>

      case "getRawSensors" =>
      case "registerRawSensor" =>
      case "getRawSensor" =>
      case "deleteRawSensor" =>

      case "loadRoot" =>
      case "getData" =>
      case "registerData" => {
        val parameters = toParameterList(myOrder)
        val mop = List(MeasurementOrParameter(
          Option(parameters.apply(1)),
          Option(parameters.apply(2)),
          Option(parameters.apply(3).toDouble),
          None,
          None,
          None,
          Option(parameters.apply(4).toLong),
          None))
        val root = new Root(None, None, None, None, Option(mop.toSeq))
        val exists = ifExists(parameters.apply(1), _backend push (parameters.apply(1), root))

        exists match{
          case "Success" => doNotify(root, parameters.apply(1), _registry)
          case _ =>
        }
        return exists
        /*
        Root(None,None,None,None,Some(List(MeasurementOrParameter(Some(JohnTab_Accelerom
        eterY),Some(m/s2),Some(-0.34476504),None,None,None,Some(1374220611),None))))

         registerData(JohnTab_AccelerometerY, m/s2, -0.34476504, 1374220611)
         registerData(JohnTab_AccelerometerX, m/s2, 42, 137)
         */
      }
    }
    null
  }

  def toParameterList(data: String): List[String] = {
    data.split("\\(|, |,|\\)").toList
  }


  def getFunctionName(order: String): String = {
    order.substring(0, order.indexOf("("))
  }

  private def ifExists(name: String, lambda: => Unit) = {
    if (_backend exists name){
      lambda
      "Success"
    }
    else
      "Unknown sensor database [" + name + "]"
  }

  def doNotify(root: Root, sensor: String, reg: SubscriptionRegistry) {
    val subscription = reg pull(("sensor", sensor))
    subscription match{
      case None =>
      case Some(x) => x.protocol match{
        case None => ProtocolFactory.createProtocol("http").send(root, subscription, sensor)
        case Some(p) => ProtocolFactory.createProtocol(p).send(root, subscription, sensor)
      }
    }
  }
}
