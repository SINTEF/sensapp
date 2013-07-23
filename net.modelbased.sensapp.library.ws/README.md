# SensApp WebSocket Library

This library allow the notification service to work with webSockets.

There are some steps before the notifier can notify webSocket clients.


First, the client has to be connected to the server (a webSocket Server is running on SensApp on port 9000).

Then, the client have to apply to a topic (the data coming from a sensor) by sending the message:

    registerToTopic=#topic

    (with #topic the id of the topic, easily found at .../sensapp/notification/registered/sensorName)

Here is an example of a basic interaction:

    connect("ws://localhost:9000")
    -> successfully connected

    registerToTopic=298669b8-21f7-4124-bfdb-7aebda5a890e
    -> successfully registered

    /* Wait for someone to push data on this topic */
    -> {data}

    disconnect
    ->

    close
    ->


The client must set the topic to be notified at every new connection. When disconnected it will no longer be nofified.

The client can also ask the server to work for it, it can call server methods in WS:

    getNotifications()
    -> JsonString: List[Subscription]

    registerNotification(JsonString: Subscription)
    -> JsonString: Subscription

    getNotification(name)
    -> JsonString: Subscription

    deleteNotification(name)
    -> Boolean (true = success)

    updateNotification(JsonString: Subscription)
    -> JsonString: Subscription


    getRawSensors()
    -> JSonString: List[SensorDatabaseDescriptor]

    registerRawSensor(JsonString: CreationRequest)
    -> Boolean (true = success)

    getRawSensor(name)
    -> JSonString: SensorDatabaseDescriptor

    deleteRawSensor(name)
    -> Boolean (true = success)


    loadRoot(JsonString: Root)
    -> String


    getData(name, from, to, sorted, limit, factorized, every, by)
    -> JsonString: Root

    getDataJson(JsonString: SearchRequest)
    -> JsonString: Root

    registerData(JsonString: Root)
    -> JsonString: List[MeasurementOrParameter]
