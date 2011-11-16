package net.modelbased.sensapp.rrd.datastore

import net.modelbased.sensapp.datastore.MongoDBSpecific
import com.mongodb.casbah.MongoConnection

/**
 * Created by IntelliJ IDEA.
 * User: ffl
 * Date: 15.11.11
 * Time: 15:09
 * To change this template use File | Settings | File Templates.
 */

object RRDMongoDBDataStore {

  override val databaseName = "sensapp_db"
  override val collectionName = "rrddata"

  def createDB(id : String)


  /**
   * The underlying MongoDB collection
   */
  @MongoDBSpecific
  private  lazy val _collection = {
    val conn = MongoConnection()
    val db = conn(databaseName)
    val col = db(collectionName)
    col
  }
}