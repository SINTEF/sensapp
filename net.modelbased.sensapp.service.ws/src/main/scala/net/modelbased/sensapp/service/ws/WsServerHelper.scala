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
import net.modelbased.sensapp.service.notifier.data.SubscriptionJsonProtocol.format
import net.modelbased.sensapp.service.database.raw.data.RequestsProtocols._
import net.modelbased.sensapp.library.senml.export.JsonProtocol._


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
        val parameters = toParameterList(myOrder)
        if(parameters.size != 5)
          return "Usage: registerNotification(name, hookList, protocol, id)"
        val subscription:Subscription = buildSubscription(parameters)

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
      }

      case "getNotification" => {
        val parameters = toParameterList(myOrder)
        val name = parameters.apply(1)
        ifExists(name, {(_registry pull ("sensor", name)).get.toJson.prettyPrint})
      }

      case "deleteNotification" => {
        val parameters = toParameterList(myOrder)
        val name = parameters.apply(1)
        ifExists(name, {
          val subscr = (_registry pull ("sensor", name)).get
          _registry drop subscr
          "true"
        })
      }

      case "updateNotification" => {
        val parameters = toParameterList(myOrder)
        if(parameters.size != 5)
          return "Usage: updateNotification(name, hookList, protocol, id)"
        val name = parameters.apply(1)
        val subscription:Subscription = buildSubscription(parameters)
        if (subscription.sensor != name) {
          "Request content does not match URL for update"
        } else {
          ifExists(name, {
            _registry push subscription; subscription.toJson.prettyPrint
          })
        }
      }

      case "dispatch" => {
        val parameters = toParameterList(myOrder)
        null
      }


      case "getRawSensors" => {
        (_backend.content map { s => _backend.describe(s, URLHandler.build("/databases/raw/sensors/").toString).get}).toJson.prettyPrint
      }

      case "registerRawSensor" => {
        val parameters = toParameterList(myOrder)
        if(parameters.size != 4)
          return "Usage: registerRawSensor(name, baseTime, schema)"
        val req:CreationRequest = buildCreationRequest(parameters)
        if (_backend exists req.sensor){
          "A sensor database identified as ["+ req.sensor +"] already exists!"
        } else {
          (_backend create req).toJson.prettyPrint
        }
      }

      case "getRawSensor" => {
        val parameters = toParameterList(myOrder)
        val name = parameters.apply(1)
        ifExists(name, {
          (_backend describe(name, URLHandler.build("/databases/raw/data/").toString)).toJson.prettyPrint
        })
      }

      case "deleteRawSensor" => {
        val parameters = toParameterList(myOrder)
        val name = parameters.apply(1)
        (_backend delete name).toJson.prettyPrint
      }


      case "loadRoot" => {
        val parameters = toParameterList(myOrder)
        null
      }

      case "getData" => {
        val parameters = toParameterList(myOrder)
        if(parameters.size == 2){
          val name = parameters.apply(1)
          val (from, to, sorted, limit, factorized, every, by) = ("0", "now", "non", -1, false, 1, "avg")
          val dataset = (_backend get(name, buildTimeStamp(from), buildTimeStamp(to), sorted, limit)).sampled(every, by).head
          if (factorized) dataset.factorized.head.toJson.prettyPrint else dataset.toJson.prettyPrint
        } else {
          val request:SearchRequest = buildSearchRequest(parameters)
          val from = buildTimeStamp(request.from)
          val to = buildTimeStamp(request.to)
          val sort = request.sorted.getOrElse("none")
          val limit = request.limit.getOrElse(-1)
          val existing = request.sensors.par.filter{ _backend exists(_) }
          (_backend get(existing.seq, from, to, sort, limit)).toJson.prettyPrint
        }
      }

      case "registerData" => {
        val parameters = toParameterList(myOrder)
        if(parameters.size != 5)
          return "Usage: registerData(name, unit, value, time)"
        val mop = List(MeasurementOrParameter(
          Option(parameters.apply(1)),
          Option(parameters.apply(2)),
          Option(parameters.apply(3).toDouble),
          None,
          None,
          None,
          Option(parameters.apply(4).toLong),
          None))
        val root = buildRootMessage(mop.toSeq)

        ifExists(parameters.apply(1), {
          val result = _backend push (parameters.apply(1), root)
          doNotify(root, parameters.apply(1), _registry)
          result.toList.toJson.prettyPrint
        })

        /*
        Root(None,None,None,None,Some(List(MeasurementOrParameter(Some(JohnTab_Accelerom
        eterY),Some(m/s2),Some(-0.34476504),None,None,None,Some(1374220611),None))))

         registerData(JohnTab_AccelerometerY, m/s2, -0.34476504, 1374220611)
         registerData(JohnTab_AccelerometerX, m/s2, 42, 137)
         */
      }

      case _ => null
    }
  }

  def toParameterList(data: String): List[String] = {
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

  private[this] def buildRootMessage(mops: Seq[MeasurementOrParameter]): Root = {
    Root(None, None, None, None, Option(mops))
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

  private def buildSubscription(params: List[String]): Subscription = {
    Subscription(params.apply(1), buildList(params.apply(2)), Option(params.apply(3)), Option(params.apply(4)))
  }

  private def buildSearchRequest(params: List[String]): SearchRequest = {
    SearchRequest(
      buildSeq(params.apply(1)),
      params.apply(2),
      params.apply(3),
      Option(params.apply(4)),
      Option(params.apply(5).toInt))
  }

  private def buildCreationRequest(params: List[String]): CreationRequest = {
    CreationRequest(params.apply(1), params.apply(2).toLong, params.apply(3))
  }

  private def buildSeq(s: String): Seq[String] = {
    s.split(", |,").toSeq
  }

  private def buildList(s: String): List[String] = {
    s.split(", |,").toList
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
