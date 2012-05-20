# SensApp Raw Registry Service

This service implement a sensor registry, to be used to handle sensors over a given SensApp instance

## Using the service

### Maven dependency

The following dependency is required to use this service in a SensApp system.

    <dependency>
  	  <groupId>net.modelbased.sensapp.service</groupId>
  	  <artifactId>net.modelbased.sensapp.service.registry</artifactId>
  	  <version>0.0.1-SNAPSHOT</version>
    </dependency>
 
### Software Dependencies

The service assumes a [MongoDB](http://www.mongodb.org/) server running on localhost.

## Available Endpoints

In the following, the provided links assume the system running on localhost.

### Sensor registry: /registry/sensors
 
#### GET /registry/sensors

 - http://localhost:8080/registry/sensors

Returns a list of stored sensors:

    Status Code: 200
    Content-Length: 75
    Server: Jetty(8.1.3.v20120416)
    Content-Type: application/json; charset=ISO-8859-1
    
    ["/registry/sensors/myVeryOwnSensor", "/registry/sensors/myOtherSensor"]

#### POST /registry/sensors

This endpoint supports the registration of a sensor in SensApp. It accepts a JSON representation of a `CreationRequest`, which defines: 

  - the identifier of the sensor (must be a valid SENML identifier),
  - a description of the sensor,
  - information about the database backend:
    - the backend to be used (_e.g._ "raw", "rrdb"),
    - the tenplate to used in this specific backend
    - an optional "baseTime" timestamp to be used as reference time by this sensor in the database 

For example, the following request asks for the registration of a sensor named `myVeryOwnSensor`, using a `raw` database to store `Numerical` data.

    Request Url: http://localhost:8080/registry/sensors
    Request Method: POST
    Status Code: 200
    
    {
      "id": "myVeryOwnSensor", "descr": "A sample sensor",
      "schema": { "backend": "raw", "template": "Numerical"}
    }  

The server answers the URLs to be used to access to this sensor in the registry

    Status Code: 200
    Content-Length: 33
    Server: Jetty(8.1.3.v20120416)
    Content-Type: text/plain
    
    /registry/sensors/myVeryOwnSensor
    
If a sensor with the exact same name already exists, the servers abort the request with a `Conflict` response code.

    Status Code: 409
    Content-Length: 67
    Server: Jetty(8.1.3.v20120416)
    Content-Type: text/plain
    
    A SensorDescription identified as [myVeryOwnSensor] already exists!

### Sensor Description: /registry/sensors/%NAME

This endpoint allows one to access to the description of the sensor registered with `%NAME` as identifier. The service returns a `NotFound` status if one try to access to an unknown sensor.

    Status Code: 404
    Content-Length: 41
    Server: Jetty(8.1.3.v20120416)
    Content-Type: text/plain
    
    Unknown sensor database [myUnknownSensor]

#### GET /registry/sensors/%NAME

  - http://localhost:8080/registry/sensors/myVeryOwnSensor
  
Returns a description of the sensor. This descriptions contains: 

  - the identifier of the sensor,
  - its description,
  - information about its data backend:
    - the kind (_e.g._, "raw", "rrdb")
    - the URL to be used to obtain a description of the sensor database
    - the URL to be used to handle data associated to this sensor
  - a creation timestamp (seconds since EPOCH)
  - some additional informations (a tags key-value object, an optional updateRate and an optional localization)

    Status Code: 200
    Content-Length: 281
    Server: Jetty(8.1.3.v20120416)
    Content-Type: application/json; charset=ISO-8859-1
    
    {
      "id": "myVeryOwnSensor", "descr": "A sample sensor",
      "backend": {
        "kind": "raw",
        "descriptor": "/databases/raw/sensors/myVeryOwnSensor",
        "dataset": "/databases/raw/data/myVeryOwnSensor"
      },
      "creation_date": 1337509216, 
      "infos": { "tags": {}}
    }


#### DELETE /registry/sensors/%NAME

Delete the sensor. It transitively deletes the backend database. This operation cannot be reversed.

    Request Url: http://localhost:8080/registry/sensors/myVeryOwnSensor
    Request Method: DELETE
    Status Code: 200

#### PUT /registry/sensors/%NAME

Updates the additional information available for this sensor. The following query defines some user-given tags (_e.g._, the owner of the sensor, the last time when the battery was changed), states that the sensor sends a new value each 60 seconds and finally declares that the sensor is located in the SINTEF ICT building in Oslo, Norway.

    Request Url: http://localhost:8080/registry/sensors/myVeryOwnSensor
    Request Method: PUT
    Status Code: 200
    
    { 
      "tags": {"owner": "seb", "batteryChanged": "2012-05-14"},
      "update_time": 60,
      "loc": { "longitude": 10.713773299999957, "latitude": 59.9452065}  
    }
               
The server returns the newly stored description:

    Status Code: 200
    Content-Length: 450
    Server: Jetty(8.1.3.v20120416)
    Content-Type: application/json; charset=ISO-8859-1
    
    {
      "id": "myVeryOwnSensor", "descr": "A sample sensor",
      "backend": {
        "kind": "raw",
        "descriptor": "/databases/raw/sensors/myVeryOwnSensor",
        "dataset": "/databases/raw/data/myVeryOwnSensor"
      },
      "creation_date": 1337511280,
      "infos": {
        "tags": { "owner": "seb", "batteryChanged": "2012-05-14"},
        "update_time": 60,
        "loc": { "longitude": 10.713773299999957, "latitude": 59.9452065}
      }
    }
