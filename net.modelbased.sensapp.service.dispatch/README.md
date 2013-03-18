# SensApp Dispatch Service

This service implements a data dispatcher for SensApp

## Using the service

### Maven dependency

The following dependency is required to use this service in a SensApp system.

    <dependency>
  	  <groupId>net.modelbased.sensapp.service</groupId>
  	  <artifactId>net.modelbased.sensapp.service.dispatch</artifactId>
  	  <version>0.0.1-SNAPSHOT</version>
    </dependency>

## Available Endpoints

### PUT /dispatch

This endpoint accepts a SENML document as input. It dispatches the content of this document to the registered sensors, according to the following principles:

  1. Canonize the received document (see [Message Canonization](http://github.com/SINTEF-9012/SensApp/tree/master/net.modelbased.sensapp.library.senml#message-canonization) )
  2. Partition the measurements according to the targeted sensors
  3. For each targeted sensors `s`:
    1. Query the `registry` service to retrieve the data backend associated to `s`
    2. Send the data associated to `s` into the data backend
    3. Notify potential listeners (see [Data Notification](http://github.com/SINTEF-9012/SensApp/tree/master/net.modelbased.sensapp.service.notifier)):
      1. Call the `notifier` service to retrieve the listeners
      2. Send the data associated to `s` for each listener
  
The service returns the name of the ignored sensors (_i.e._, the ones that are not registered in SensApp but present in the received SENML document)
