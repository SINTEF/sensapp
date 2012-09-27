# SensApp Notifier Service

This service implement a registry for client notification

## Using the service

### Maven dependency

The following dependency is required to use this service in a SensApp system.

    <dependency>
  	  <groupId>net.modelbased.sensapp.service</groupId>
  	  <artifactId>net.modelbased.sensapp.service.notifier</artifactId>
  	  <version>0.0.1-SNAPSHOT</version>
    </dependency>
 
### Software Dependencies

The service assumes a [MongoDB](http://www.mongodb.org/) server running on localhost.

## Available Endpoints

In the following, the provided links assume the system running on localhost.

### Notification registry: /notification/registered
 
#### GET /notification/registered

 - http://localhost:8080/notification/registered

Returns a list of sensors that owns listeners:

    Status Code: 200
    Content-Length: 44
    Server: Jetty(8.1.3.v20120416)
    Content-Type: application/json; charset=ISO-8859-1
    
    ["/notification/registered/myVeryOwnSensor"]

#### POST /notification/registered

This endpoint supports the registration of a sensor into the listening mechanism. The sensor declares a list of `hooks`, to be called when a data is pushed into the `dispatch` service (see [Dispatch](http://github.com/mosser/SensApp/tree/master/net.modelbased.sensapp.service.dispatch)).

For example, the following request asks for the registration of a sensor named `myVeryOwnSensor`, with a listener available on `localhost:8090/echo`:

    Request Url: http://localhost:8080/notification/registered
    Request Method: POST
    Status Code: 201
    
    { "sensor": "myVeryOwnSensor", "hooks": ["localhost:8090/echo"] }


The server answers the URLs to be used to access to this sensor in the registry

    Status Code: 201
    Content-Length: 40
    Server: Jetty(8.1.3.v20120416)
    Content-Type: text/plain
    
   /notification/registered/myVeryOwnSensor
    
If a sensor with the exact same name already exists, the servers abort the request with a `Conflict` response code.


### Notification Description: /notification/registered/%NAME

This endpoint allows one to access to the description of the sensor registered with `%NAME` as identifier. The service returns a `NotFound` status if one try to access to an unknown sensor.

    Status Code: 404
    Content-Length: 41
    Server: Jetty(8.1.3.v20120416)
    Content-Type: text/plain
    
    Unknown sensor database [myUnknownSensor]

#### GET /notification/registered/%NAME

  - http://localhost:8080/notification/registered/myVeryOwnSensor
  
Returns a description of the sensor, that is, the sensor identifier and its associated hooks. 

    Status Code: 200
    Content-Length: 69
    Server: Jetty(8.1.3.v20120416)
    Content-Type: application/json; charset=ISO-8859-1
    
    {"sensor": "myVeryOwnSensor", "hooks": ["localhost:8090/echo"] }


#### DELETE /notification/registered/%NAME

Delete the sensor notification entry. This operation cannot be reversed.

    Request Url: http://localhost:8080/notification/registered/myVeryOwnSensor
    Request Method: DELETE
    Status Code: 200

#### PUT /notification/registered/%NAME

Update the notification entry

    Request Url: http://localhost:8080/notification/registered/myVeryOwnSensor
    Request Method: PUT
    Status Code: 200
    
    {"sensor": "myVeryOwnSensor", "hooks": ["localhost:9090/echo"] }

The servers simply answers true when done.

    Status Code: 200
    Content-Length: 4
    Server: Jetty(8.1.3.v20120416)
    Content-Type: text/plain