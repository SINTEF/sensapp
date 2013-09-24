This is a module used to test the server performance.

This module tests its websocket-side-performance using Gatling.

The Gatling library 2.0.0-M3a do not support websockets.
A websocket module for Gatling has been developped for the version 1.5.2 but one
have to install it manually into the local Gatling Library.

This is how it is feasible:

* Either:

You have to go to this page https://github.com/gilt/gatling-websocket and get this project.

Now you have the websocket library, you have to compile it using sbt.
You have to be at the root of the "gatling-websocket" file and execute:

    sbt package

Then a new file has been created named "gatling-websocket-0.0.9.jar" in the folder "gatling-websocket/target".

* Or:

Copy the file of the net.modelbased.sensapp.backyard.gatling.ws project.
The file is here: /lib/.m2/repository/com/excilys/ebi/gatling/gatling-websocket/1.5.2/gatling-websocket-1.5.2.jar


Now you just have to add this file to the Gatling library. Copy and paste it here:

    .m2/repository/com/excilys/ebi/gating/gatling-websocket/1.5.2/gatling-websocket-1.5.2.jar

If the folder "gatling-websocket" does not exist, create it.
If the folder "1.5.2" does not exist, create it.
Paste the jar file into this "1.5.2" directory and rename it as "gatling-websocket-1.5.2.jar".

Now you have added websocket support to the Gatling library and every thing should work fine.