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
import net.modelbased.sensapp.service.notifier.data._
import net.modelbased.sensapp.service.notifier.protocols.ProtocolFactory
import net.modelbased.sensapp.service.database.raw.backend.impl.MongoDB
import net.modelbased.sensapp.service.database.raw.backend.Backend
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import java.util.UUID
import net.modelbased.sensapp.service.database.raw.data.{SensorDatabaseDescriptor, SearchRequest, CreationRequest}
import net.modelbased.sensapp.library.system.{TopologyFileBasedDistribution, URLHandler}
import net.modelbased.sensapp.service.notifier.data.SubscriptionJsonProtocol._
import net.modelbased.sensapp.service.database.raw.data.RequestsProtocols._
import net.modelbased.sensapp.library.senml.export.JsonProtocol._
import net.modelbased.sensapp.library.senml.export.{JsonParser => RootParser}

/**
 * Created with IntelliJ IDEA.
 * User: Jonathan
 * Date: 18/07/13
 * Time: 13:56
 */
object WsServerHelper {
  implicit val partnerName = "database.raw.ws"
  implicit val partners = new TopologyFileBasedDistribution { implicit val actorSystem = null }
  private[this] val _backend: Backend = new MongoDB()
  private[this] val _registry = new SubscriptionRegistry()

  def doOrder(order: String): String = {
    var myOrder = order
    getFunctionName(order) match{
      case "getNotifications" => {
        (_registry retrieve List()).toJson.prettyPrint
      }

      case "registerNotification" => {
        val json = getUniqueArgument(myOrder)
        val subscription = json.asJson.convertTo[Subscription]

        if (_registry exists ("sensor", subscription.sensor)){
          "A Subscription identified by ["+ subscription.sensor +"] already exists!"
        } else {
          subscription.protocol.foreach(p => {
            if(p == "ws" && !subscription.id.isDefined)
              subscription.id=Option(UUID.randomUUID().toString)
          })
          _registry push subscription
          subscription.toJson.prettyPrint
        }
        /*{"sensor": "JohnTab_AccelerometerZ","hooks": ["http://127.0.0.1:8090/echo"],"protocol": "ws"}*/

      }

      case "getNotification" => {
        val name = getUniqueArgument(myOrder)
        ifExists(name, {(_registry pull ("sensor", name)).get.toJson.prettyPrint})
      }

      case "deleteNotification" => {
        val name = getUniqueArgument(myOrder)
        ifExists(name, {
          val subscr = (_registry pull ("sensor", name)).get
          _registry drop subscr
          "true"
        })
      }

      case "updateNotification" => {
        val json = getUniqueArgument(myOrder)
        val subscription = json.asJson.convertTo[Subscription]//buildSubscription(parameters)
        ifExists(subscription.sensor, {
          _registry push subscription; subscription.toJson.prettyPrint
        })
      }

      case "dispatch" => {
        val parameters = getUniqueArgument(myOrder)
        null
      }

      case "getRawSensors" => {
        (_backend.content map { s => _backend.describe(s, URLHandler.build("/databases/raw/sensors/").toString).get}).toJson.prettyPrint
      }

      case "registerRawSensor" => {
        val json = getUniqueArgument(myOrder)
        val req = json.asJson.convertTo[CreationRequest]
        if (_backend exists req.sensor){
          "A sensor database identified as ["+ req.sensor +"] already exists!"
        } else {
          (_backend create req).toJson.prettyPrint
        }
      }

      case "getRawSensor" => {
        val name = getUniqueArgument(myOrder)
        ifExists(name, {
          (_backend describe(name, URLHandler.build("/databases/raw/data/").toString)).toJson.prettyPrint
        })
      }

      case "deleteRawSensor" => {
        val name = getUniqueArgument(myOrder)
        (_backend delete name).toJson.prettyPrint
      }


      case "loadRoot" => {
        val json = getUniqueArgument(myOrder)
        val root = RootParser.fromJson(json)
        null
      }

      case "getData" => {
        //TODO check parameters
        val parameters = argumentsToList(myOrder)
        val name = parameters.apply(1)
        val (from, to, sorted, limit, factorized, every, by) = ("0", "now", "non", -1, false, 1, "avg")
        val dataset = (_backend get(name, buildTimeStamp(from), buildTimeStamp(to), sorted, limit)).sampled(every, by).head
        if (factorized) dataset.factorized.head.toJson.prettyPrint else dataset.toJson.prettyPrint
      }

      case "getDataJson" => {
        val json = getUniqueArgument(myOrder)
        val request = json.asJson.convertTo[SearchRequest]
        val from = buildTimeStamp(request.from)
        val to = buildTimeStamp(request.to)
        val sort = request.sorted.getOrElse("none")
        val limit = request.limit.getOrElse(-1)
        val existing = request.sensors.par.filter{ _backend exists(_) }
        (_backend get(existing.seq, from, to, sort, limit)).toJson.prettyPrint
      }

      case "registerData" => {
        val json = getUniqueArgument(myOrder)
        val root = RootParser.fromJson(json)
        val name = root.baseName.get
        ifExists(name, {
          val result = _backend push (name, root)
          doNotify(root, name, _registry)
          result.toList.toJson.prettyPrint
        })
        //{"bn":"JohnTab_AccelerometerX","bt":1374064069,"e":[{"u":"m/s2","v":12,"t":156544},{"u":"m/s2","v":24,"t":957032}]}
      }

      case _ => null
    }
  }

  def getUniqueArgument(data: String): String = {
    data.split("\\(|\\)").apply(1)
  }

  def argumentsToList(data: String): List[String] = {
    data.split("\\(|, |,|\\)").toList
  }

  def getFunctionName(order: String): String = {
    order.substring(0, order.indexOf("("))
  }

  private def ifExists(name: String, lambda: => String): String = {
    if (_backend exists name)
      lambda
    else
      "Unknown sensor database [" + name + "]"
  }

  private def buildTimeStamp(str: String): Long = {
    import java.text.SimpleDateFormat
    val TimeStamp = """(\d+)""".r
    val LiteralDate = """(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d)""".r
    val Now = "now"
    str match {
      case Now => System.currentTimeMillis() / 1000
      case TimeStamp(x) => x.toLong
      case LiteralDate(lit)   => {
        val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
        val date = format.parse(lit)
        date.getTime / 1000
      }
      case _ => throw new RuntimeException("Unable to parse date ["+str+"]!")
    }
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
