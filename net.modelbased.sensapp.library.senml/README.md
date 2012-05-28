# SensApp SENML Library

This Scala library implements the [SENML] standard. For now, it provides:

  - Message _canonization_
  - Data value model
  - Compliance checker
  - JSON support (serialization / deserialization)
  

## General Information

### Maven dependency

The following dependency is required to use this service in a SensApp system.

    <dependency>
  	  <groupId>net.modelbased.sensapp.library</groupId>
  	  <artifactId>net.modelbased.sensapp.library.senml</artifactId>
  	  <version>0.0.1-SNAPSHOT</version>
    </dependency>    


### Import in your code

    import net.modelbased.sensapp.library.senml._

### Data Structure

The SENML data structures are implemented as two case classes: `Root` and `MeasurementOrParameter`. Implementation is available here: [DataModel.scala](https://github.com/mosser/SensApp/blob/master/net.modelbased.sensapp.library.senml/src/main/scala/net/modelbased/sensapp/library/senml/DataModel.scala "DataModel.scala")

As SENML relies intrinsically on field optionality, we used the Scala `Option` mechanisms. 
Thus, the `baseTime` attribute for example is not defined as a `Long`, but as an `Option[Long]`. 
At run-time, values can be `None`, or `Some(bt)`, where `bt` is the actual value you're looking for. Pattern matching (`match`/ `case`) is usually a good way to handle such elements.
If you're not familiar with this mechanism in Scala, you should have a look at the _Compliance Checkers_ implementation, available here: [Standard.scala](https://github.com/mosser/SensApp/blob/master/net.modelbased.sensapp.library.senml/src/main/scala/net/modelbased/sensapp/library/senml/Standard.scala)

## Message Canonization

SENML supports the factorization of recurring data (_e.g._, `baseTime`, `baseUnit`) in the `Root` data structure. This is extremely good to reduce the size of exchanged data, but starts to be annoying if time or data size is not a problem. Thus, we propose a _canonization_ function, which transform any SENML message into a _canonical_ representation. Basically, remove all implicit assumption (_e.g._, "no time provided means _now_", "if no _unit_ available, then the _baseUnit_ must be used") by constructing "self contained" `MeasurementOrParameter` entry.

The following SENML element models in JSON 2 values obtained from two different sensors: "my-sensor/inside" and "my-sensor/outside" (the "sensor/" prefix is factorized). Both sensors emits data using celsius degrees as measurement unit (also factorized). Time reference is set to the 100 second after the EPOCH (_i.e._, 100 seconds after the 1st of january in 1970). The first sensor ("inside") send a value of 20.2 degrees measured 20 seconds before the time reference, and the second ("outside") send a value of -8.8 measured 10 seconds before the time reference. The message assumes a SENML version equals to 1, as no one is provided.

    {
      "bn": "my-sensor/", "bu": "degC", "bt": 100,
      "e": [ {"n": "inside",  "v": 20.2, "t": -20}, {"n": "outside", "v": -8.8, "t": -10} ]
    }

The associated canonical representation is the following:

    {"ver": 1, "e": [ {n: "my-sensor/inside",  "v": 20.2, "u": "degC", "t": 80}, {n: "my-sensor/outside", "v": -8.8, "u": "degC", "t": 90}]}

Let `root` an instance of `Root`. Its canonical representation is computed on the fly (using parallelized code for large messages) and obtained through the `root.canonized` method.

## Data Value model

The SENML standard basically provides a way to model 4 different types of data value, reified as the following in the SENML library: `FloatDataValue`, `StringDataValue`, `BooleanDataValue` and `SumDataValue`. For a given `MeasurementOrParameter` instance called `mop`, one can easily discriminate its content using the `mop.data` attribute:

    mop.data match {
      case FloatDataValue(d)   => "I contain a float: " + d
      case StringDataValue(s)  => "I contain a string: " + s
      case BooleanDataValue(b) => "I contain a boolean: " + b
      case SumDataValue(d,i)   => "I contain a summed value: " + d + " and an optional instant value: " + i
    }

## Compliance Checkers

The SENML standard defines several constraints on the way data structure can be filled. We implemented the following constraints that are automatically checked on the SENML elements modelled through this library. The library will automatically reject (with an `IllegalArgumentException`) malformed structures. We use here the version #8 of the IETF standard draft.


## JSON 

Serialization and deserialization mechanisms are implemented in the `JsonParser` singleton. 

    import net.modelbased.sensapp.library.senml.{Root, JsonParser}
    val obj: Root = JsonParser.fromJson("{ ... }")
    val json: String = JsonParser.toJson(obj)
    
### Extra: Spray Support

As SensApp (the core project) relies on the [Spray] framework for its REST layer, we also propose spray _protocols_ to allow automatic marshalling/unmarshalling in Spray. One can activate these protocols by importing the implicit functions defined in the `JsonProtocol` singleton.

    import net.modelbased.sensapp.library.senml.JsonProtocol._


  [SENML]: http://tools.ietf.org/html/draft-jennings-senml-08
  [Spray]: http://spray.cc