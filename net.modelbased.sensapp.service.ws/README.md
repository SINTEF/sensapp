# SensApp WebSocket Parsing Message Function

This class implement the function that parses messages sent to the server.

It allows clients to ask the server to perform tasks.

A notification topic is set on a sensor. If a specific sensor is a notification topic then every data sent to the server
for this sensor will be notified to all clients which subscribed to the topic.

Here are the accepted actions:

## Notifications

* Get all notification topics registered into the server

    <pre><code>getNotifications()
    -> JsonString: List[Subscription]
    </pre></code>

* Subscribe to a given notification topic

    <pre><code>getNotified(notificationId: Int)
    -> "You are now registered for the topic: " + notificationId
    </pre></code>

* Register a new notification topic

    <pre><code>registerNotification(JsonString: Subscription)
    -> JsonString: Subscription
    </pre></code>

* Get a notification topic

    <pre><code>getNotification(name: String)
    -> JsonString: Subscription
    </pre></code>

* Delete a notification topic

    <pre><code>deleteNotification(name: String)
    -> Boolean (true = success)
    </pre></code>

* Update a notification topic

    <pre><code>updateNotification(JsonString: Subscription)
    -> JsonString: Subscription
    </pre></code>

## Dispatch

* Send data to register to the server

    <pre><code>dispatch(JsonString)
    -> JsonString
    </pre></code>

## Raw database

* Get all sensors

    <pre><code>getRawSensors()
    -> JSonString: List[SensorDatabaseDescriptor]
    </pre></code>

* Register a new sensor

    <pre><code>registerRawSensor(JsonString: CreationRequest)
    -> Boolean (true = success)
    </pre></code>

* Get a sensor

    <pre><code>getRawSensor(name: String)
    -> JSonString: SensorDatabaseDescriptor
    </pre></code>

* Delete a sensor

    <pre><code>deleteRawSensor(name: String)
    -> Boolean (true = success)
    </pre></code>

* Load sensor data

    <pre><code>loadRoot(JsonString: Root)
    -> String
    </pre></code>

* Get data

    <pre><code>getData(name: String, from: String, to: String, sorted: String, limit: String, factorized: String, every: String, by: String)
    -> JsonString: Root
    </pre></code>

    <pre><code>getDataJson(JsonString: SearchRequest)
    -> JsonString: Root
    </pre></code>

* Register data

    <pre><code>registerData(JsonString: Root)
    -> JsonString: List[MeasurementOrParameter]
    </pre></code>

## Composites

* Get all composites

    <pre><code>getComposites()
    -> JsonString: List[CompositeSensorDescription]
    </pre></code>

* Register a composite

    <pre><code>registerComposite(JsonString: CompositeSensorDescription)
    -> JsonString: CompositeSensorDescription
    </pre></code>

* Get a composite

    <pre><code>getComposite(name: String)
    -> JsonString: CompositeSensorDescription
    </pre></code>

* Delete a composite

    <pre><code>deleteComposite(name: String)
    -> Boolean (true = success)
    </pre></code>

* Update composite sensors

    <pre><code>updateCompositeSensors(name: String, JsonString: SensorList)
    -> JsonString: CompositeSensorDescription
    </pre></code>

* Update composite sensor tags

    <pre><code>updateCompositeSensorTags(name: String, JsonString: SensorTags)
    -> JsonString: CompositeSensorDescription
    </pre></code>

* Update composite description

    <pre><code>updateCompositeSensorTags(name: String, JsonString: DescriptionUpdate)
    -> JsonString: CompositeSensorDescription
    </pre></code>

## Sensors

* Register a sensor

    <pre><code>registerSensor(JsonString: RegistryCreationRequest)
    -> sensorUrl: String
    </pre></code>

* Get all sensors

    <pre><code>getSensors()
    -> JsonString: List[SensorDescription]
    </pre></code>

* Get a sensor

    <pre><code>getSensor(name: String)
    -> JsonString: SensorDescription
    </pre></code>

* Delete a sensor

    <pre><code>deleteSensor(name: String)
    -> Boolean (true = success)
    </pre></code>

* Update a sensor

    <pre><code>updateSensor(name: String, JsonString: DescriptionUpdate)
    -> JsonString: SensorDescription
    </pre></code>


