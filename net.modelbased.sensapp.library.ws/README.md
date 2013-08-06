# SensApp WebSocket Library

This library allows the use of WebSockets in SensApp.

The two sides are implemented in this library: the server and the client.
Server and Client can be instantiate using the factory WsServerFactory or WsClientFactory which allows only one instance.

## Running the server

This is a basic server allowing connection, disconnection, message receiving and sending. When receiving a message, it
is sent to a parsing function implemented [here]. This function returns the response to give to this message, of null
if the message is not allowed.

* To run the server:

    var port = 9000
    var webSocketServer = WsServerFactory.makeServer(port)
    webSocketServer.start()

Now, the server is running and you can connect as much client as you wish.

## Running the client

The client is also a very basic one allowing connection, disconnection, message receiving and sending. A basic "console"
client is implemented [here].

* To run a client:

    var server = "ws://127.0.0.1:9000"
    var serverUri = URI.create(server)
    val client = WsClientFactory.makeClient(serverUri)
    client.connect()

## Proof that the connection has been made

If the connection is possible, the client is connecting to the server.
* The server will display:

    New client connected

* And the client will display:

    opened connection

## Sending message

Now you can use:

    send("MyStringMessage")

from the server to send this message to the client. You can use the same call to send the message from the client to
the server.

* If the message is received on the server, it will display:

    Received Message String: MyStringMessage

* If the client received the message, it will display:

    received: MyStringMessage







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
