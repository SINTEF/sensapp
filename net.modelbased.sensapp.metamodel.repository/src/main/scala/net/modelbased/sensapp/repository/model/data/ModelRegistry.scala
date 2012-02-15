package net.modelbased.sensapp.repository.model.data

import net.modelbased.sensapp.datastore._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.MongoDBObjectBuilder

class ModelRegistry extends DataStore[Model] {

  override val databaseName = "sensapp_db"
  override val collectionName = "models.registry" 
    
  override def identify(m: Model) = ("name", m.name)
  
  override def deserialize(dbObj: DBObject): Model = {
    Model(dbObj.as[String]("name"), dbObj.as[String]("content"))
  }
 
  override def serialize(obj: Model): DBObject = {
    val builder = MongoDBObject.newBuilder
    builder += ("name" -> obj.name)
    builder += ("content" -> obj.content)
    builder.result
  }
    
}