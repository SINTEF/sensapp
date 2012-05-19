# SensApp Raw Database Service

This service implement a _raw_ database, _i.e._, a database able to store raw data obtained from given sensors.

## Using the service

### Maven dependency

The following dependency is required to use this service in a SensApp system.

    <dependency>
  	  <groupId>net.modelbased.sensapp.service</groupId>
  	  <artifactId>net.modelbased.sensapp.service.database.raw</artifactId>
  	  <version>0.0.1-SNAPSHOT</version>
    </dependency>
 
### Software Dependencies

The service assumes a [MongoDB](http://www.mongodb.org/) server running on localhost.

## Available Endpoints

In the following, the provided links assume the system running on localhost.

### Sensor database repository: /databases/raw/sensors

The repository stores descriptions of sensor databases, and support their creation.

#### GET /databases/raw/sensors

  - http://localhost:8080/databases/raw/sensors

Returns the list of sensor databases (URLs to the associated resources) declared in this repository.

    Status Code: 200
    Content-Length: 50
    Server: Jetty(8.1.2.v20120308)
    Content-Type: application/json; charset=ISO-8859-1
    
    ["/databases/raw/sensors/my-pretty-little-sensor"]


#### POST /databases/raw/sensors

Considering a request body that describes the database to register, perform database registration.

Query example:

    Request Url: http://localhost:8080/databases/raw/sensors
    Request Method: POST
    Status Code: 201
    Params: {}
    
    {
      "sensor": "my-pretty-little-sensor",
      "baseTime": 1334821847,
      "schema": "Numerical"
    }

The server answers the URL of the created resource:

    Status Code: 201
    Content-Length: 46
    Server: Jetty(8.1.2.v20120308)
    Content-Type: text/plain

    /databases/raw/sensors/my-pretty-little-sensor

If a sensor database with the exact same name already exists, the servers abort the request with a `Conflict` response code.

    Status Code: 409
    Content-Length: 73
    Server: Jetty(8.1.2.v20120308)
    Content-Type: text/plain
    
    A sensor database identified as [my-pretty-little-sensor] already exists!


### Sensor database description: /databases/raw/sensors/%NAME

We consider here that `%NAME` is the name of an existing sensor database. The service always (_i.e._, for any HTTP method used) returns a `NotFound` status code when requested on an unknown name.

    Status Code: 404
    Content-Length: 43
    Server: Jetty(8.1.2.v20120308)
    Content-Type: text/plain
    
    Unknown sensor database [my-unknown-sensor]
    

#### GET /databases/raw/sensors/%NAME

  - http://localhost:8080/databases/raw/sensors/my-pretty-little-sensor

Return a description of the database, using a JSON format. The `data_lnk` attributes gives a reference to the "data" resource of this database

    Status Code: 200
    Content-Length: 140
    Server: Jetty(8.1.2.v20120308)
    Content-Type: application/json; charset=ISO-8859-1
    
    {
      "sensor": "my-pretty-little-sensor",
      "schema": "Numerical",
      "size": 0,
      "data_lnk": "/databases/raw/data/my-pretty-little-sensor"
    }

#### DEL /databases/raw/sensors/%NAME

Delete the database, including all data. This operation is definitive, and cannot be restored through any SensApp mechanism. 
As SensApp does not handle security in its core, it is up to the server configuration to restrict this command.

Query example: 

    Request Url: http://localhost:8080/databases/raw/sensors/my-pretty-little-sensor
    Request Method: DELETE
    Status Code: 200
    Params: {}


### Sensor data handling: /databases/raw/data/%NAME

Data are exchanged as SENML document. The server will systematically reject malformed document received as input with a `BadRequest` code:

Bad query example: 

    Request Url: http://localhost:8080/databases/raw/data/my-pretty-little-sensor
    Request Method: PUT
    Status Code: 400
    Params: {}
    
    { "e": [{}] }


Obtained response:

    Status Code: 400
    Content-Length: 125
    Server: Jetty(8.1.2.v20120308)
    Content-Type: text/plain
    
    The request content was malformed:
    requirement failed: As 'baseName' is not provided, all measurements must provides a 'name'

#### GET /databases/raw/data/%NAME

Return **all** the data stored for this sensor

Obtained response:
 
    Status Code: 200
    Content-Length: 31574
    Server: Jetty(8.1.2.v20120308)
    Content-Type: application/json; charset=ISO-8859-1

    {
      "bn": "my-pretty-little-sensor",
      "bt": 1337416727,
      "e": [{
        "u": "m",
        "v": 0.14098352193832397,
        "t": 0
      }, ... ]
    }
    
One can use parameters (`from`, `to`) to restrict the retrieved data to a given interval. The parameter `to` is optional (default value: `"now"`). These parameters can take the following values:

  - "now": will be translated into current server time
  - "yyyy-MM-ddTHH:mm:ss": a human readable date (_e.g._, `"2012-05-19T10:42:09"`)
  - "[0-9]+": number of seconds since EPOCH (_e.g._, `1337416929`, equivalent to the previous date)

Examples:

  - http://localhost:8080/databases/raw/data/my-pretty-little-sensor?from=2012-05-19T10:42:09
  - http://localhost:8080/databases/raw/data/my-pretty-little-sensor?from=2012-05-19T10:42:09&to=2012-05-19T10:42:11
  - http://localhost:8080/databases/raw/data/my-pretty-little-sensor?from=1337416929&to=1337416931
  - http://localhost:8080/databases/raw/data/my-pretty-little-sensor?from=1337416929
    
#### PUT /databases/raw/data/%NAME

Push data to be stored for this sensor. Data must be represented as a SENML document. The elements that are not relevant for this sensor (_i.e._, associated to a sensor ­ `%NAME`) are ignored. The list of ignored elements is returned to the user.

Query example:

    Request Url: http://localhost:8080/databases/raw/data/my-pretty-little-sensor
    Request Method: PUT
    Status Code: 200
    
    {
      "bn": "my-pretty-little-",
      "e": [ {"n": "sensor",  "u": "m", "v": 1.2, "t": 1337438916 }, 
             {"n": "unknown", "u": "m", "v": 1.2, "t": 1337438916 } ]
    }  

Obtained response

    Status Code: 200
    Content-Length: 87
    Server: Jetty(8.1.2.v20120308)
    Content-Type: application/json; charset=ISO-8859-1
    
    [{"n": "unknown", "u": "m", "v": 1.2, "t": 1337438916 }]



  