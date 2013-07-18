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
 * Module: net.modelbased.sensapp.library.ws
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
package net.modelbased.sensapp.library.ws.Server

import net.modelbased.sensapp.library.ws.Server.data.{SensorDescriptionRegistry, SensorInformation}


/**
 * Created with IntelliJ IDEA.
 * User: Jonathan
 * Date: 18/07/13
 * Time: 13:56
 */
object WsServerHelper {

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
        myOrder = myOrder.substring(myOrder.indexOf("("))
        val name = myOrder.substring(1, myOrder.indexOf(","))
        myOrder = myOrder.substring(myOrder.indexOf(","))
        var data = myOrder.substring(1, myOrder.indexOf(")"))
        if(data.charAt(0) == ' ')
          data = data.substring(1)

        val sensor = (_registry pull ("id", name)).get
        /*val info = new SensorInformation()
        val safe = SensorInformation(info.tags.filter( t => t._1 != "" ), info.updateTime, info.localization)
        sensor.infos = safe
        _registry push sensor */
        return sensor.toString
      }
    }
    null
  }

  private[this] val _registry = new SensorDescriptionRegistry()

  def getFunctionName(order: String): String = {
    order.substring(0, order.indexOf("("))
  }
}
