# SensApp Datastore Library

This library implements a generic datastore. Based on limited information given by the user,
it provides an automatic interface to handle persistent object using MongoDB

## General Information

### Maven dependency

The following dependency is required to use this service in a SensApp system.

    <dependency>
  	  <groupId>net.modelbased.sensapp.service</groupId>
  	  <artifactId>net.modelbased.sensapp.library.datastore</artifactId>
  	  <version>0.0.2-SNAPSHOT</version>
    </dependency>    
    
### Software Dependencies

The service assumes a [MongoDB](http://www.mongodb.org/) server running on localhost.

### Import in your code

    import net.modelbased.sensapp.library.datastore.DataStore

## Implementing a registry

We consider here a simple `Element` class that implements key -> value pairs, to be handled in a persistent way.

    case class Element(key: Int, value: String)


To create a `DataStore` associated to this class, the following information are needed:

  - The name of the database to be used in the MongoDB server
  - The name of the collection to be used to store the elements
  - A method to uniquely identify elements among others
    - _e.g._, the `key` is unique, so for a given `e: Element`, the tuple `("key", e.key)` is unique 
  - A method to transform a JSON document into an `Element`
    - _e.g._, `{"key": "k", "value": "v"}` will be transformed into `Element("k", "v")`
  - A method to transform an `Element` into a JSON Document
    - _e.g._, `Element("k", "v")` will be transformed into `{"key": "k", "value": "v"}`
    
This is achieved by the following code:

    class ElementRegistry extends DataStore[Element]  {
      override val databaseName = "myDatabase"
      override val collectionName = "sample.elements" 
      override def identify(e: Element) = ("key", e.key)
      override def deserialize(json: String): Element = { ... }
      override def serialize(e: Element): String = { ... }
    }

## Using the DataStore

### The Criterion structure

A `Criterion` is a key-value pair `(String, Any)`, used to categorize object based on their JSON representation used in the database. For example, the previously described method `identify` returns a `Criterion` that uniquely identify a given object in the database.

Criteria are used to look for object in the MongoDB backend. 

### Operations provided by a DataStore

#### Element existence

  - `exists(id: Criterion): Boolean`

This method returns true if the criterion identifies an element, false elsewhere

#### Element pulling

  - `pull(id: Criterion): Option[T]` 

This method returns `None` if the criterion does not identify an object, and `Some(element)` elsewhere

#### Element pushing

  - `push(obj: T)`
  
This method pushes the given object in the database. If an object with the same identifier (reuslt of the `identify` method) already exists in the collection, it is silently removed and the new one replace it.

#### Element search

  - `retrieve(criteria: List[Criterion]): List[T]`

Returns a list of objects that match the given criteria (criterion are composed with an _and_ semantics).

#### Element droppping

  - `drop(obj: T)`: Remove the object from the collection. 
  - `dropAll()`: reset the collection (all data are dropped)


## JSON / Object marshalling using Spray

One can use the spray-json framework to automatically handle the json marshalling.

    import cc.spray.json._
    object ElementJsonProtocol extends DefaultJsonProtocol {
      implicit val format = jsonFormat(Element, "key", "value")
    }

Considering that the json protocol is correctly imported in the current scope, one can use the 
following implementation for the serialize/deserialize methods

    override def deserialize(json: String): Element = { json.asJson.convertTo[Element] }
    override def serialize(e: Element): String = { e.toJson.toString }



