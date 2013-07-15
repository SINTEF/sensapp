# SensApp First Sprint System

This system is the first release of the SensApp system

## Prerequisite

This system assumes the following things:

  - An open (no user, no password) MongoDB database up and running on localhost
  - A Servlet container supporting aysnchronous servlets (_e.g._, Jetty 8) 

## Using the system

To start the system in development mode (_i.e._, running the system in a self-contained Jetty server), 
one can use the following Maven command:

    $ cd net.modelbased.sensapp.system.sprints.first/
    $ mvn jetty:run
    ...

To deploy the system in a properly configured server environment, just package the `war` file using Maven:

    $ cd net.modelbased.sensapp.system.sprints.first/
    $ mvn package

The `net.modelbased.sensapp.system.sprints.first-0.0.1-SNAPSHOT.war` file is generated in the `./target` directory.
    
## Contained Services

  - Sensor Databases:
    - [Raw](http://github.com/SINTEF-9012/SensApp/tree/master/net.modelbased.sensapp.service.database.raw)
  - SensApp Services:
    - [Registry](http://github.com/SINTEF-9012/SensApp/tree/master/net.modelbased.sensapp.service.registry)
    - [Dispatch](http://github.com/SINTEF-9012/SensApp/tree/master/net.modelbased.sensapp.service.dispatch)
    - [Notifier](http://github.com/SINTEF-9012/sensapp/tree/master/net.modelbased.sensapp.service.notifier)
  
