# Demonstration scenario

  - author:  SŽbastien Mosser <sebastien.mosser@sintef.no>
  - date:    24.05.2012
  - context: Focus team #1, end of sprint #2

## Objectives:

  1. Show how to start SensApp in development mode
  2. Show how to register sensors in SensApp
  3. Show how to push data in SensApp
  4. Show hot to add listeners to sensors


## Preparation

Clear the local database, if any:
  
    mongo sensapp_db
      > db.notifications.drop()
      > db.raw.data.drop()
      > db.raw.metadata.drop()
      > db.registry.sensors.drop()
      > exit 


Start

  - Several terminals:
    - `srv-terminal`: bash, with pwd = SensApp root directory
    - `echo`: sensapp echo service:
      - cd cd net.modelbased.sensapp.backyard.echo
      - mvn jetty:run
  - Several REST Console pointing to
    - `sensors-list`: `http://localhost:8080/registry/sensors`
    - `in-sens`: `http://localhost:8080/registry/sensors/my-sensor/inside`
    - `in-data`: `http://localhost:8080/databases/raw/data/my-sensor/inside`
    - `dispatch`: `http://localhost:8080/dispatch`
    - `notif`: `http://localhost:8080/notification/registered`
    - `echo`: `http://localhost:8090/echo`

REST console configuration:

  - Target: 
    - Accept, Content-type: "text/plain,application/json"
  - Body:
    - Content Headers, Content-type: "application/json"
    - Request Payload, Raw Body: ticked. Use this field to POST or PUT data

# Starting SensApp in development mode 
 
In `srv-terminal`:

    cd net.modelbased.sensapp.system.sprints.first
    mvn jetty:run 


# Describing sensors into SensApp

## Declaring sensors in the registry

First, we register a sensor to store inside temperature. In
`sensors-list`, POST the following message: 

    {
      "id": "my-sensor/inside", 
      "descr": "inside temperature",
      "schema": { 
        "backend": "raw", 
        "template": "Numerical"
      }
    } 

> ==>> **The service answers the URL of the stored resource**.

Then, we register a sensor to store outside temperature. In
`sensors-list`, POST the following message: 

    {
      "id": "my-sensor/outside", 
      "descr": "outside temperature",
      "schema": { 
        "backend": "raw", 
        "template": "Numerical"
      }
    } 

## Retrieving sensor description

We can retrieve information about the sensor. In `in-sens`, GET.

> ==>> **The service replies the description of the resource, including information about the data-intensive `backend` and a time-stamp storing the date where the sensor have been created**.

## Updating sensor description

  - One can add meta-data to a given sensor. 
    - All meta-data are optional. 
  - Fixed meta-data are 
    - update_time` (in seconds),
    -  and `loc` (GPS coordinates).  
  - User-given meta-data (key-value pairs) are stored in `tags` 

We add meta-data to the previous sensor. In `in-sens`, PUT the following request

    { 
      "tags": {
        "owner": "sebastienm", 
        "activationDate": "2012-05-14"
      },
      "update_time": 60,
      "loc": { 
        "longitude": 10.713773299999957, 
        "latitude": 59.9452065
      }  
    }

> ==>> **The service replies the updated description**.

> ==>> **The data back-end and the creation time-stamp cannot be changed**


# Pushing data into SensApp

## Using the SenML dispatcher 

The SensApp dispatcher handles SenML messages and dispatch these
messages into the database.


In `dispatch`, POST the following request

    { 
      "bn": "my-sensor/", "bu": "degC",
      "bt": 1337843674, 
      "e": [ 
        { "n": "inside",  "v": 22.0, "t": -540},
        { "n": "outside", "v": 23.5, "t": -540},
        { "n": "unknown", "v": 12.3, "t": -540},
        { "n": "inside",  "v": 22.0, "t": -480},
        { "n": "outside", "v": 23.7, "t": -480},
        { "n": "unknown", "v": 12.2, "t": -480},
        { "n": "inside",  "v": 22.1, "t": -420},
        { "n": "outside", "v": 23.5, "t": -420},
        { "n": "unknown", "v": 12.0, "t": -420},
        { "n": "inside",  "v": 22.1, "t": -360},
        { "n": "outside", "v": 23.6, "t": -360},
        { "n": "unknown", "v": 11.7, "t": -360},
        { "n": "inside",  "v": 22.1, "t": -300},
        { "n": "outside", "v": 23.7, "t": -300},
        { "n": "unknown", "v": 11.6, "t": -300},
        { "n": "inside",  "v": 22.1, "t": -240},
        { "n": "outside", "v": 23.8, "t": -240},
        { "n": "unknown", "v": 11.5, "t": -240},
        { "n": "inside",  "v": 222, "t": -180},
        { "n": "outside", "v": 23.8, "t": -180},
        { "n": "unknown", "v": 11.5, "t": -180},
        { "n": "inside",  "v": 22.2, "t": -120},
        { "n": "outside", "v": 23.9, "t": -120},
        { "n": "unknown", "v": 11.5, "t": -120},
        { "n": "inside",  "v": 22.2, "t": -60},
        { "n": "outside", "v": 24.0, "t": -60},
        { "n": "unknown", "v": 11.4, "t": -60},
        { "n": "inside",  "v": 22.3, "t": 0},
        { "n": "outside", "v": 23.9, "t": 0},
        { "n": "unknown", "v": 11.4, "t": 0}
      ]
    }

> ==>> **The service replies the identifier of the sensors that are not registered in SensApp but still put resources in it**.

## Getting the data stored for a given sensor

Data are retrieve through their backend. In `in-data`, GET.

> ==>> **Measurements are not ordered**.

One can restrict the time interval. In `in-data`, change the URL to:
  - http://localhost:8080/databases/raw/data/my-sensor/inside?from=1337843474&to=1337843594

> ==>> **One can also use human-readable time-stamp in the URL**.

One can also use POST to query multiple sensors at the same time:

    {
      "sensors": [
        "unknown", 
        "my-sensor/inside", 
        "my-sensor/outside" 
      ],
      "from": "2012-05-24T09:13:00",
      "to": "now"
    }

## Updating data 

The data back-end follows an update semantics. Here, we fix the stupid
_223_ value stored at `t = 1337843494`.

In `dispatch`, put the following message

    {"e": [
      { "n": "my-sensor/inside", "v": 22.3, 
        "u": "degC", "t": 1337843494}
      ]}

One can see in `in-data` (GET) that the value is now updated.


# Notification mechanism

## Registering a notification application

One can register listeners for given sensors

In `notif`, POST the following message

    { "sensor": "my-sensor/inside", 
      "hooks": ["localhost:8090/echo"] 
    }

In `notif`, POST the following message

    { "sensor": "my-sensor/outside", 
      "hooks": ["localhost:8090/echo"] 
    }


## The echo service


In `echo, PUT the following message:
    
    
     
    #######################
    ## PLEASE PRINT THIS ##
    #######################
    
    
    




> ==> **the echo service print the received message on `stdout`**.

## Send new data

In `dispatch`, PUT the following message:

    { 
      "bn": "my-sensor/",
      "bu": "degC", "bt": 1337843674,
      "e": [
        {"n": "inside",  "v": 22.3, "t": 60 },
        {"n": "outside", "v": 24.0, "t": 60 }
      ]
    }


> ==>> **The echo service was activated twice, one per sensor**.

 
That's all folks.
