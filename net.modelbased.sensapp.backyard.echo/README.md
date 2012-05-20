# SensApp Echo Service

This service basically prints on `stdout` any data received (as `post` or `put`) on its endpoint (`/echo`).

To start the service on `localhost:8090`:

    mosser@azrael:SensApp $ cd net.modelbased.sensapp.backyard.echo/
    mosser@azrael:net.modelbased.sensapp.backyard.echo $ mvn jetty:run
    