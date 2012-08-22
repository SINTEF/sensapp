/**
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: Sebastien Mosser <sebastien.mosser@sintef.no>
 *
 * Module: net.modelbased.sensapp.service.converter
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
package net.modelbased.sensapp.service.converter


import au.com.bytecode.opencsv.CSVReader
import cc.spray.http._
import cc.spray.json._
import cc.spray.json.DefaultJsonProtocol._
import cc.spray.directives._
import cc.spray.typeconversion.SprayJsonSupport
import cc.spray.io.IoWorker
import cc.spray.client.HttpConduit
import cc.spray.can.client.HttpClient
import cc.spray.RequestContext
import cc.spray.client.Get
import java.io.StringReader
import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.Locale
import java.text.NumberFormat
import net.modelbased.sensapp.library.system.{Service => SensAppService, URLHandler}
import net.modelbased.sensapp.library.senml.{MeasurementOrParameter, Root, DataType, SumDataValue, DoubleDataValue, StringDataValue, BooleanDataValue}
import net.modelbased.sensapp.library.senml.export.JsonParser
import net.modelbased.sensapp.service.converter.request._
import net.modelbased.sensapp.service.converter.request.CSVDescriptorProtocols._
import net.modelbased.sensapp.service.converter.request.CSVExportDescriptorProtocols._
import scala.collection.mutable.Buffer
import scala.collection.JavaConversions._
import scala.collection.mutable.StringBuilder
import akka.actor._
import akka.dispatch._
import akka.util.duration._
import java.net.URL
import net.modelbased.sensapp.library.senml.export.JsonProtocol




trait Service extends SensAppService {
  
  override implicit val partnerName = "converter"
    
  implicit val system = ActorSystem()
  val ioWorker = new IoWorker(system).start()
  def httpClientName = "converter-client"
  val httpClient = system.actorOf(
    props = Props(new HttpClient(ioWorker)),
    name = httpClientName
  )
    
  private[this] val _registry = new CSVDescriptorRegistry()
  
  private def ifExists(context: RequestContext, name: String, lambda: => Unit) = {
    if (_registry exists ("desc", name))
      lambda
    else
      context fail(StatusCodes.NotFound, "Unknown descriptor [" + name + "]") 
  }
    
  
  val service = {
    path("converter" / "toCSV") {
      post {
        content(as[CSVExportDescriptor]) { exportDesc => context =>
          val csv = generateCSV(exportDesc)
          context complete csv
        } 
      } ~ cors("POST")
    } ~
    path("converter" / "toSRT") {
      post {
        content(as[CSVExportDescriptor]) { exportDesc => context =>
          val srt = generateSRT(exportDesc)
          context complete srt
        } 
      } ~ cors("POST")
    } ~
    path("converter" / "fromCSV") {
      post {
        content(as[CSVDescriptor]) { desc => context =>
          if (_registry exists ("name", desc.name)){
            context fail (StatusCodes.Conflict, "A CSV descriptor identified by ["+ desc.name +"] already exists!")
          } else {
            _registry push desc
            context complete (StatusCodes.Created, URLHandler.build("/converter/fromCSV/" + desc.name))
          }
        }
      } ~
      get { 
        parameter("flatten" ? false) { flatten =>  context =>
          val data = (_registry retrieve(List()))
          if (flatten) {
            context complete data
          } else {
            context complete (data map { r => URLHandler.build("/converter/fromCSV/" + r.name) })
          }
        }
      } ~ cors("GET", "POST")
    } ~
    path("converter" / "fromCSV" / "[a-zA-Z0-9][a-zA-Z0-9-:._/\\[\\]]*".r) { name =>
      get { context =>
        ifExists(context, name, {context complete (_registry pull ("desc", name)).get})
      } ~
      delete { context =>
        ifExists(context, name, {  
          val desc = (_registry pull ("desc", name)).get
          _registry drop desc 
          context complete "true"
        })
      } ~
      post { 
        content(as[String]) { rawData => context =>
          ifExists(context, name, {
            val desc = (_registry pull ("desc", name)).get
            val status = parseCSV(desc, rawData)
            context complete status.factorized.collect{case root => JsonParser.toJson(root)}.mkString("\n")
          })
        }
      } ~ 
      put {
        content(as[CSVDescriptor]) { desc => context => 
          if (desc.name != name) {
            context fail(StatusCodes.Conflict, "Request content does not match URL for update")
          } else {
            ifExists(context, name, { _registry push(desc); context complete desc })
	      } 
        }
      } ~ cors("GET", "PUT", "POST", "DELETE")
    }
  }
  
  def getRemoteRoots(exportDesc : CSVExportDescriptor) : List[Root] = {
    exportDesc.datasets/*.par*/.flatMap{dataset => 
        val url = new URL(dataset.url)
        val conduit = if (url.getPort > 0)
          new HttpConduit(httpClient, host = url.getHost, port = url.getPort) {val rootPipeline = simpleRequest ~> sendReceive ~> unmarshal[Root]}
        else
          new HttpConduit(httpClient, host = url.getHost) {val rootPipeline = simpleRequest ~> sendReceive ~> unmarshal[Root]}
        val responseFuture : Future[Root] = conduit.rootPipeline(Get(url.getPath))
        try {
          val root = Await.result(responseFuture, 600 second)
          root.factorized
        } catch { case e : Exception =>
          println("TIMEOUT: " + url + "\n caused by: " + e.getClass)
          println(" trace:")
          e.printStackTrace()
          Nil
        } finally {conduit.close}
      }.toList.sortBy(_.baseName)
  }
  
  def generateSRT(exportDesc : CSVExportDescriptor) : String = {
	val roots : List[Root] = getRemoteRoots(exportDesc)
    
    val builder : StringBuilder = new StringBuilder()
    
    //TODO: manage unroll strategy by distributing string values evenly between two timestamps
    
	val mopsByTime = roots.par.map(_.canonized).seq.collect{case root => root.measurementsOrParameters}.toList.flatten.flatten.groupBy(mop => mop.time.getOrElse(-1l)).filterKeys(k=> k > -1).toSeq.sortWith(_._1 < _._1)
	
	var i = 0
	var minTime = 0l
	var previousTime = minTime
	
    mopsByTime.foreach{
	  case (t, mops) if (i == 0) =>
	    minTime = t
	    previousTime = minTime
	    i = i + 1
      case (t, mops) if (i > 0) => 
    	builder append i + "\r\n"
    	i = i + 1
    	var delta = previousTime - minTime
    	previousTime = t    	
    	builder append ("%02d:%02d:%02d,%03d".format(delta/3600, (delta%3600)/60, delta%60, 0))
    	builder append " --> "
    	delta = t - minTime
    	builder append ("%02d:%02d:%02d,%03d".format(delta/3600, (delta%3600)/60, delta%60, 0)) + "\r\n"
    	mops.sortBy(_.name.get).foreach{mop =>
    	  //builder append "<font color=#FFFF00><b>"
    	  builder append mop.name.get + ": "
    	  //builder append "</b></font>"
   	      builder.append(mop.data match {
            case DoubleDataValue(d)   => "%.2f".format(d)
            case StringDataValue(s)  => s //TODO: manage unroll strategy somewhere around here
            case BooleanDataValue(b) => b
            case SumDataValue(d,i)   => "%.2f".format(d)
            case _ => "-"
          })
          builder append "\r\n"
    	}
    	builder append "\r\n"
    }
	builder append "\r\n"
    builder.toString    
  }
  
  def generateCSV(exportDesc : CSVExportDescriptor) : String = {      
	//List of factorized roots (one sensor/root, measurements ordered by timestamp) sorted by base name
    val roots = getRemoteRoots(exportDesc)
    
    val csv = CSV(roots.collect{case r => r.baseName.get}, exportDesc.separator.getOrElse(","))	
	val index : Map[Int, String] = roots.collect{case r => r.baseName.get}.zipWithIndex.map{_.swap}.toMap
	    
    roots.par.map(_.canonized).seq.collect{case root => root.measurementsOrParameters}.toList.flatten.flatten.groupBy(mop => mop.time.getOrElse(-1l)).filterKeys(k=> k > -1).toSeq.sortWith(_._1 < _._1).sliding(2,1).foreach{pair => 
      val t = pair.head._1
      val mops = pair.head._2
      mops.foreach{ mop =>
        mop.data match {
   	      case StringDataValue(s)  =>
     	    val values = s.split(";")
     	    if (values.size > 1) pair.last match {case (t2, mops2) =>
   	          val delta = (t2-t)/values.size * 1000
   	          var time = t*1000 - delta
   	          values.foreach{ v => 
   	            time = t*1000 + delta
   	            csv.push(time, mop.name.get, v)
   	          }
   	        }
   	        else {csv.push(t*1000, mop.name.get, s)}
   	      case d : DataType => d match {
            case DoubleDataValue(d)   => csv.push(t*1000, mop.name.get, d.toString)
            case BooleanDataValue(b) => csv.push(t*1000, mop.name.get, b.toString)
            case SumDataValue(d,i)   => csv.push(t*1000, mop.name.get, d.toString)
     	  }
    	}
      }
    }
    csv.toString
  }

  
  
  def parseCSV(request : CSVDescriptor, rawData : String) : Root = {
    val start = System.currentTimeMillis
    
    
    val separator = request.separator.getOrElse(',')
    val escape = request.escape.getOrElse('\\')
    
    val reader : CSVReader = new CSVReader(new StringReader(rawData), separator, escape, 0);
    val myEntries = reader.readAll();
    
    
    val getTimestamp: String => Option[Long] = (s : String) => try { 
      request.timestamp.format match {
        case None =>  
          val t = s.toLong / 1000
          Some(t)
        case Some(format) => {
          val dateFormat = new SimpleDateFormat(format.pattern, new Locale(format.locale))
          val t = dateFormat.parse(s).getTime() / 1000
          Some(t)
        }
      } 
    } catch { case _ =>
      println("Cannot extract timestamp from " + s)
      None 
    }
    
    val extractDouble: (String, String) => Option[Double] = (data, col : String) => try {
      val cleanSplit = data.trim.split("^(0+)(\\d+.\\d+)")//removes leading zeros (e.g. as generated in Torque files) preventing proper parsing
      val clean = (if (cleanSplit.length > 2) cleanSplit(2) else data.trim)
      val cleanDouble = if (request.locale.isDefined) {
        val format = NumberFormat.getInstance(new Locale(request.locale.get));
    	format.parse(clean).doubleValue()
      } else {
        clean.toDouble
      }
      Some(cleanDouble)
    } catch { case _ =>
        println("Cannot parse value " + data +", ignoring measurement " + col)
        None
    }
    
    //CSV file grouped by (parseable) timestamps
    val chunks : Map[Long, Buffer[Array[String]]] = myEntries.groupBy(line => getTimestamp(line(request.timestamp.columnId).trim).getOrElse(-1l)).filterKeys(k => k>0)
    
    val extract: (List[Array[String]], Long) => List[MeasurementOrParameter] = (chunk : List[Array[String]], timestamp : Long) => request.columns.flatMap{col =>
      if (col.strategy.isDefined)
    	  col.strategy.get match {
    	  	case "chunk" => Some(MeasurementOrParameter(Some(col.name), Some(col.unit), None, Some(chunk.collect{case cols => cols(col.columnId)}.mkString(", ")), None, None, Some(timestamp), None))
            case strategy : String =>
              val values = chunk.flatMap{line =>
                val data = line(col.columnId)
                extractDouble(data, col.name)
              }
              strategy match {
                case "min" => Some(MeasurementOrParameter(Some(col.name), Some(col.unit), Some(values.sortWith(_ < _).head), None, None, None, Some(timestamp), None))
                case "max" => Some(MeasurementOrParameter(Some(col.name), Some(col.unit), Some(values.sortWith(_ > _).head), None, None, None, Some(timestamp), None))
                case "avg" => Some(MeasurementOrParameter(Some(col.name), Some(col.unit), Some((values.sum)/values.size), None, None, None, Some(timestamp), None))
                case "one" => Some(MeasurementOrParameter(Some(col.name), Some(col.unit), Some(values.head), None, None, None, Some(timestamp), None))
            }
          }
      else {//
        col.kind match {
          case "number" => extractDouble(chunk.head(col.columnId), col.name).flatMap(number => Some(MeasurementOrParameter(Some(col.name), Some(col.unit), Some(number), None, None, None, Some(timestamp), None)))
          case "string" =>  Some(MeasurementOrParameter(Some(col.name), Some(col.unit), None, Some(chunk.collect{case cols => cols(col.columnId)}.mkString(";")), None, None, Some(timestamp), None))
          case "boolean" =>  Some(MeasurementOrParameter(Some(col.name), Some(col.unit), None, None, Some(chunk.head(col.columnId).trim=="true"), None, Some(timestamp), None))
          case "sum" => extractDouble(chunk.head(col.columnId), col.name).flatMap(number => Some(MeasurementOrParameter(Some(col.name), Some(col.unit), None, None, None, Some(number), Some(timestamp), None)))
          case _ => 
            println("Kind " + col.kind + " does not exist, ignoring measurement " + col.name)
            None
        }
      }
    }
    
    val raw = chunks/*.par*/.flatMap{ case (timestamp, lines) => extract(lines.toList, timestamp) }.toIndexedSeq.sortWith(_.time.getOrElse(0l) < _.time.getOrElse(0l))
        
    Root(request.baseName, None, None, None, Some(raw))
  }  
}