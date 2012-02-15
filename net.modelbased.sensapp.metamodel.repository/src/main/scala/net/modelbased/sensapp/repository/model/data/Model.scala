package net.modelbased.sensapp.repository.model.data

import cc.spray.json._

/**
 * Case class implementing the domain model
 */
case class Model (val name: String, val content: String)


/**
 * implicit function to marshal a Model into a JSON object
 */
object ModelJsonProtocol extends DefaultJsonProtocol {
  implicit val modelFormat = jsonFormat(Model, "name", "content")
}


