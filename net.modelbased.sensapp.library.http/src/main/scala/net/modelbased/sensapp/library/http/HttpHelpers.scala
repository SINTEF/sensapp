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
 * Module: net.modelbased.sensapp.library.http
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
package net.modelbased.sensapp.library.http

import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpPost, HttpPut}
import scala.collection.JavaConversions._
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.HttpResponse
import org.apache.http.util.EntityUtils
import scala.io.Source
import org.apache.http.HttpRequest
import org.apache.http.client.methods.HttpRequestBase

/**
 * Helper object to handle interaction with a RESTful service through HTTP
 * 
 * @autor Sebastien Mosser
 */
object HttpHelpers {
  
  /**
   * Retrieve the value of a given header in a response
   * @param response the retrieved response
   * @param key the name of the header (e.g., "Location")
   * @return None if the header is not defined, Some(header) in other cases
   */
  def getHeaderValue(response: HttpResponse, key: String): Option[String] = {
    val headers = response.getHeaders(key)
    if (headers.isEmpty) { None } else { Some(headers(0).getValue()) }
  }
  
  /**
   * Retrieve the status code of a given response
   * @param response the retrieved response
   * @return the status code, as an integer
   */
  def getStatusCode(response: HttpResponse): Int = {
    response.getStatusLine().getStatusCode()
  }
  
  /**
   * Retrieve the content of a response (i.e., its body part)
   * @param response the retrieved response
   * @param key the name of the header (e.g., "Location")
   * @return a string that contains the response body
   */
  def getBodyContent(response: HttpResponse): String = {
    Source.fromInputStream(response.getEntity().getContent()).mkString
  }
  
  /**
   * Retrieve the content type of a response
   * @param response the retrieved response
   * @return the content type of the response 
   */
  def getContentType(response: HttpResponse): String = {
    response.getEntity().getContentType().getValue()
  }
  
  /**
   * Post data (parameters) through the execution of a POST query on an HTTP client
   * We use UTF-8 to encode the parameters
   * @param client the HTTP client to be used
   * @param post the HttpPost query to be executed
   * @param data a (Key, Value) list to be posted
   * @return a HttpResponse received from the server
   */
  def postData(client: DefaultHttpClient, post: HttpPost, data: List[(String,String)]): HttpResponse = {
    val pairs = data map { case (k,v) => new BasicNameValuePair(k,v) }
    val entity = new UrlEncodedFormEntity(pairs,"UTF-8")
    post.setEntity(entity)
    client.execute(post)
  }
  
  /**
   * Put data (parameters) through the execution of a PUT query on an HTTP client
   * We use UTF-8 to encode the parameters
   * @param client the HTTP client to be used
   * @param put the HttpPut query to be executed
   * @param data a (Key, Value) list to be posted
   * @return a HttpResponse received from the server
   */
  def putData(client: DefaultHttpClient, post: HttpPut, data: List[(String,String)]): HttpResponse = {
    val pairs = data map { case (k,v) => new BasicNameValuePair(k,v) }
    val entity = new UrlEncodedFormEntity(pairs,"UTF-8")
    post.setEntity(entity)
    client.execute(post)
  }
  
  /**
   * Release all the resources used by a response.
   * 
   * This method <strong>MUST</strong> me used to release resource (an exception will be thrown if not)
   */
  def release(response: HttpResponse) { EntityUtils.consume(response.getEntity()) }
  
}