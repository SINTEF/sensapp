# SensApp WebSocket Library

This library allow the notification service to work with webSockets.
There are some steps before the notifier can notify webSocket clients.

First, the client has to be connected to the server (a webSocket Server is running on SensApp on port 9000).
Then, the client have to apply to a topic (the data coming from a sensor) by sending the message:
    thisIsMyId<#topic>
    (with #topic the id of the topic, easily found at .../sensapp/notification/registered/sensorName)

It's done, the server is now notifying the client for every data sent to the server on this/these topics.
