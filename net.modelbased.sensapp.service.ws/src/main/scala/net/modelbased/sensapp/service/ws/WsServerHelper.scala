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
import net.modelbased.sensapp.library.senml.export.JsonProtocol._
import net.modelbased.sensapp.library.senml.export.{JsonParser => RootParser}
import net.modelbased.sensapp.library.system.TopologyFileBasedDistribution
import net.modelbased.sensapp.service.notifier.protocols.ProtocolFactory
import net.modelbased.sensapp.service.notifier.data.{SubscriptionRegistry, Subscription}
import net.modelbased.sensapp.service.notifier.data.SubscriptionJsonProtocol._
import net.modelbased.sensapp.service.database.raw.backend.impl.MongoDB
import net.modelbased.sensapp.service.database.raw.backend.Backend
import net.modelbased.sensapp.service.database.raw.data.{SensorDatabaseDescriptor, SearchRequest, CreationRequest}
import net.modelbased.sensapp.service.database.raw.data.RequestsProtocols._
import net.modelbased.sensapp.service.registry.data.ElementJsonProtocol.{
  compositeSensorDescription, sensorList, sensorTags, descriptionUpdate, sensorDescription, schema}
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import cc.spray.json.DefaultJsonProtocol.{jsonFormat => baseJsonFormat}
import java.util.UUID
import net.modelbased.sensapp.service.registry.data.{CreationRequest => RegistryCreationRequest}
import net.modelbased.sensapp.service.registry.data.CompositeSensorDescription
import net.modelbased.sensapp.service.registry.data.SensorList
import net.modelbased.sensapp.service.notifier.data.Subscription
import net.modelbased.sensapp.service.database.raw.data.SearchRequest
import net.modelbased.sensapp.library.senml.Root
import scala.Some
import net.modelbased.sensapp.service.registry.data.CompositeSensorDescriptionRegistry
import net.modelbased.sensapp.service.registry.data.SensorDescriptionRegistry
import net.modelbased.sensapp.service.registry.data.SensorTags
import net.modelbased.sensapp.service.registry.data.Schema
import net.modelbased.sensapp.service.registry.data.DescriptionUpdate
import net.modelbased.sensapp.service.registry.data.{Backend => RegistryBackend}
import net.modelbased.sensapp.service.registry.BackendHelper
import net.modelbased.sensapp.library.ws.Server.WsServerFactory
import org.java_websocket.WebSocket
import net.modelbased.sensapp.service.notifier.Helper

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
  private[this] val _subscriptionRegistry = new SubscriptionRegistry()
  private[this] val _compositeRegistry = new CompositeSensorDescriptionRegistry()
  private[this] val _sensorRegistry = new SensorDescriptionRegistry()
  implicit val registryCreationRequest = baseJsonFormat(RegistryCreationRequest, "id", "descr", "schema")

  def doOrder(order: String, ws: WebSocket = null): String = {
    val myOrder = order
    getFunctionName(order) match{
      case "getNotifications" => {
        sendClient(myOrder, (_subscriptionRegistry retrieve List()).toJson.prettyPrint)
      }

      case "getNotified" => {
        val notificationId = getUniqueArgument(myOrder)
        WsServerFactory.myServer.addClientFromMessage(notificationId, ws)
        sendClient(myOrder, "You are now registered for the topic: "+notificationId)
      }

      case "registerNotification" => {
        val json = getUniqueArgument(myOrder)
        val subscription = json.asJson.convertTo[Subscription]

        if (_subscriptionRegistry exists ("sensor", subscription.sensor)){
          sendClient(myOrder, "A Subscription identified by ["+ subscription.sensor +"] already exists!")
        } else {
          subscription.protocol.foreach(p => {
            if(p == "ws" && !subscription.id.isDefined)
              subscription.id=Option(UUID.randomUUID().toString)
          })
          _subscriptionRegistry push subscription
          sendClient(myOrder, subscription.toJson.prettyPrint)
        }
        /*{"sensor": "JohnTab_AccelerometerZ","hooks": ["http://127.0.0.1:8090/echo"],"protocol": "ws"}*/

      }

      case "getNotification" => {
        val name = getUniqueArgument(myOrder)
        sendClient(myOrder, ifSensorExists(name, {(_subscriptionRegistry pull ("sensor", name)).get.toJson.prettyPrint}))
      }

      case "deleteNotification" => {
        val name = getUniqueArgument(myOrder)
        ifSensorExists(name, {
          val subscr = (_subscriptionRegistry pull ("sensor", name)).get
          _subscriptionRegistry drop subscr
          sendClient(myOrder, "true")
        })
      }

      case "updateNotification" => {
        val json = getUniqueArgument(myOrder)
        val subscription = json.asJson.convertTo[Subscription]
        ifSensorExists(subscription.sensor, {
          _subscriptionRegistry push subscription
          sendClient(myOrder, subscription.toJson.prettyPrint)
        })
      }

      case "dispatch" => {
        val data = getUniqueArgument(myOrder)
        try{
          data.asJson.convertTo[Subscription]
          return doOrder("registerNotification("+data+")")
        } catch { case e =>  }
        try{
          data.asJson.convertTo[CreationRequest]
          return doOrder("registerRawSensor("+data+")")
        } catch { case e =>  }
        try{
          RootParser.fromJson(data)
          return doOrder("registerData("+data+")")
        } catch { case e =>  }
        try{
          data.asJson.convertTo[CompositeSensorDescription]
          return doOrder("registerComposite("+data+")")
        } catch { case e =>  }
        null
      }

      case "getRawSensors" => {
        sendClient(myOrder, (_backend.content map { s =>
          _backend.describe(s, "sensapp/databases/raw/sensors/").get
        }).toJson.prettyPrint)
      }

      case "registerRawSensor" => {
        val json = getUniqueArgument(myOrder)
        val req = json.asJson.convertTo[CreationRequest]
        if (_backend exists req.sensor){
          sendClient(myOrder, "A sensor database identified as ["+ req.sensor +"] already exists!")
        } else {
          sendClient(myOrder, (_backend create req).toJson.prettyPrint)
        }
      }

      case "getRawSensor" => {
        val name = getUniqueArgument(myOrder)
        ifSensorExists(name, {
          sendClient(myOrder, (_backend describe(name, "sensapp/databases/raw/data/")).get.toString.toJson.prettyPrint)
        })
      }

      case "deleteRawSensor" => {
        val name = getUniqueArgument(myOrder)
        sendClient(myOrder, (_backend delete name).toJson.prettyPrint)
      }

      case "loadRoot" => {
        val json = getUniqueArgument(myOrder)
        val root = RootParser.fromJson(json)
        try {
          val start = System.currentTimeMillis()
          _backend importer root
          val delta = System.currentTimeMillis() - start
          sendClient(myOrder, "processed in %sms".format(delta))
        } catch {
          case e => sendClient(myOrder, e.toString)
        }
      }

      case "getData" => {
        val parameters = argumentsToList(myOrder)
        if(parameters.size != 9)
          return sendClient(myOrder, ("""Usage: getData(name, from, to, sorted, limit, factorized, every, by)
                  |  (for set default argument, put null)
                  """.stripMargin))
        val (name, from, to, sorted, limit, factorized, every, by) = setQueryValues(parameters)
        val dataset = (_backend get(name, buildTimeStamp(from), buildTimeStamp(to), sorted, limit)).sampled(every, by).head
        if (factorized)
          sendClient(myOrder, dataset.factorized.head.toJson.prettyPrint)
        else
          sendClient(myOrder, dataset.toJson.prettyPrint)
      }

      case "getDataJson" => {
        val json = getUniqueArgument(myOrder)
        val request = json.asJson.convertTo[SearchRequest]
        val from = buildTimeStamp(request.from)
        val to = buildTimeStamp(request.to)
        val sort = request.sorted.getOrElse("none")
        val limit = request.limit.getOrElse(-1)
        val existing = request.sensors.par.filter{ _backend exists(_) }
        sendClient(myOrder, (_backend get(existing.seq, from, to, sort, limit)).toJson.prettyPrint)
      }

      case "registerData" => {
        val json = getUniqueArgument(myOrder)
        val root = RootParser.fromJson(json)
        val name = root.baseName.get
        ifSensorExists(name, {
          val result = _backend push (name, root)
          Helper.doNotify(root, name, _subscriptionRegistry)
          sendClient(myOrder, result.toList.toJson.prettyPrint)
        })
        //{"bn":"JohnTab_AccelerometerX","bt":1374064069,"e":[{"u":"m/s2","v":12,"t":156544},{"u":"m/s2","v":24,"t":957032}]}
      }

      case "getComposites" => {
        sendClient(myOrder, _compositeRegistry.retrieve(List()).par.seq.toList.toJson.prettyPrint)
      }

      case "registerComposite" => {
        val json = getUniqueArgument(myOrder)
        val request = json.asJson.convertTo[CompositeSensorDescription]
        if (_compositeRegistry exists ("id", request.id)){
          sendClient(myOrder, "A CompositeSensorDescription identified as ["+ request.id +"] already exists!")
        } else {
          _compositeRegistry push (request)
          sendClient(myOrder, "sensapp/registry/composite/sensors/"+ request.id)
        }
      }

      case "getComposite" => {
        val name = getUniqueArgument(myOrder)
        sendClient(myOrder, ifCompositeExists(name, {(_compositeRegistry pull ("id", name)).get.toJson.prettyPrint}))
      }

      case "deleteComposite" => {
        val name = getUniqueArgument(myOrder)
        ifCompositeExists(name, {
          val sensor = _compositeRegistry pull ("id", name)
          _compositeRegistry drop sensor.get
          sendClient(myOrder, "true")
        })
      }

      case "updateCompositeSensors" => {
        val parameters = argumentsToList(myOrder)
        if(parameters.size != 3)
          return sendClient(myOrder, """Usage: updateCompositeSensors(name, JsonString: SensorList)""".stripMargin)
        val (name, list) = (parameters.apply(1), parameters.apply(2).asJson.convertTo[SensorList])
        ifCompositeExists(name, {
          val sensor = (_compositeRegistry pull ("id", name)).get
          sensor.sensors = list.sensors
          _compositeRegistry push sensor
          sendClient(myOrder, sensor.toJson.prettyPrint)
        })
      }

      case "updateCompositeSensorTags" => {
        val parameters = argumentsToList(myOrder)
        if(parameters.size != 3)
          return sendClient(myOrder, """Usage: updateCompositeSensorTags(name, JsonString: SensorTags)""".stripMargin)
        val (name, tags) = (parameters.apply(1), parameters.apply(2).asJson.convertTo[SensorTags])
        ifCompositeExists(name, {
          val sensor = (_compositeRegistry pull ("id", name)).get
          sensor.tags = Some(tags.tags.filter( t => t._1 != "" ))
          _compositeRegistry push sensor
          sendClient(myOrder, sensor.toJson.prettyPrint)
        })
      }

      case "updateCompositeDescription" => {
        val parameters = argumentsToList(myOrder)
        if(parameters.size != 3)
          return sendClient(myOrder, """Usage: updateCompositeDescription(name, JsonString: DescriptionUpdate)""".stripMargin)
        val (name , request) = (parameters.apply(1), parameters.apply(2).asJson.convertTo[DescriptionUpdate])
        ifCompositeExists(name, {
          val sensor = (_compositeRegistry pull ("id", name)).get
          sensor.description = request.description
          _compositeRegistry push sensor
          sendClient(myOrder, sensor.toJson.prettyPrint)
        })
      }

      case "registerSensor" => {
        val json = getUniqueArgument(myOrder)
        val req = json.asJson.convertTo[RegistryCreationRequest]
        val backend = createDatabase(req.id, req.schema)
        _sensorRegistry push (req.toDescription(backend))
        sendClient(myOrder, "/registry/sensors/" + req.id)
      }

      case "getSensors" => {
        val descriptors =  _sensorRegistry.retrieve(List()).par
        sendClient(myOrder, descriptors.seq.toList.toJson.prettyPrint)
      }

      case "getSensor" => {
        val name = getUniqueArgument(myOrder)
        sendClient(myOrder, ifSensorExists(name, {(_sensorRegistry pull ("id", name)).get.toJson.prettyPrint}))
      }

      case "deleteSensor" => {
        val name = getUniqueArgument(myOrder)
        ifSensorExists(name, {
          val sensor = _sensorRegistry pull ("id", name)
          delDatabase(name)
          _sensorRegistry drop sensor.get
          sendClient(myOrder, "true")
        })
      }

      case "updateSensor" => {
        val parameters = argumentsToList(myOrder)
        if(parameters.size != 3)
          return sendClient(myOrder, """Usage: updateSensorDescription(name, JsonString: DescriptionUpdate)""".stripMargin)
        val (name , request) = (parameters.apply(1), parameters.apply(2).asJson.convertTo[DescriptionUpdate])
        ifSensorExists(name, {
          val sensor = (_sensorRegistry pull ("id", name)).get
          sensor.description = request.description
          _sensorRegistry push sensor
          sendClient(myOrder, sensor.toJson.prettyPrint)
        })
      }

      case _ => null
    }
  }

  /**
   * This function return the only argument of a string function call
   * @param data: The String to parse. Look like "function(argument)"
   * @return argument defined up here
   */
  def getUniqueArgument(data: String): String = {
    data.split("\\(|\\)").apply(1)
  }

  /**
   * This function split the string in a list of strings containing functionName and then all the parameters
   * @param data: The string to be parsed. Look like "function(arg0, arg1, arg2...)"
   * @return (function, arg0, arg1, arg2...)
   */
  def argumentsToList(data: String): List[String] = {
    data.split("\\(|, |,|\\)").toList
  }

  def getFunctionName(order: String): String = {
    order.substring(0, order.indexOf("("))
  }

  private def setQueryValues(list: List[String]): (String, String, String, String, Int, Boolean, Int, String)={
    var (name, from, to, sorted, limit, factorized, every, by) =
      ("", "0", "now", "none", -1, false, 1, "avg")
    if(list.apply(1) != "null") name = list.apply(1)
    if(list.apply(2) != "null") from = list.apply(2)
    if(list.apply(3) != "null") to = list.apply(3)
    if(list.apply(4) != "null") sorted = list.apply(4)
    if(list.apply(5) != "null") limit = list.apply(5).toInt
    if(list.apply(6) != "null") factorized = list.apply(6).toBoolean
    if(list.apply(7) != "null") every = list.apply(7).toInt
    if(list.apply(8) != "null") by = list.apply(8)
    (name, from, to, sorted, limit, factorized, every, by)
  }

  private def ifSensorExists(name: String, lambda: => String): String = {
    if (_backend exists name)
      lambda
    else
      "Unknown sensor database [" + name + "]"
  }

  private def ifCompositeExists(name: String, lambda: => String): String = {
    if (_compositeRegistry exists ("id", name))
      lambda
    else
      "Unknown composite database [" + name + "]"
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

  /*def doNotify(root: Root, sensor: String, reg: SubscriptionRegistry) {
    val subscription = reg pull(("sensor", sensor))
    subscription match{
      case None =>
      case Some(x) => x.protocol match{
        case None => ProtocolFactory.createProtocol("http").send(root, subscription, sensor)
        case Some(p) => ProtocolFactory.createProtocol(p).send(root, subscription, sensor)
      }
    }
  } */

  private def sendClient(order: String, response: String): String={
    order + ", " + response
  }

  private[this] def createDatabase(id: String, schema: Schema): RegistryBackend = {
    val helper = BackendHelper(schema)
    val urls = helper.createDatabase(id, schema, partners)
    RegistryBackend(schema.backend, urls._1, urls._2)
  }

  private[this] def delDatabase(id: String) = {
    val backend = (_sensorRegistry pull ("id", id)).get.backend
    val helper = BackendHelper(backend)
    helper.deleteDatabase(backend, partners)
  }
}
