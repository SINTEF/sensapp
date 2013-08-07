# SensApp WebSocket Library

This library allows the use of WebSockets in SensApp.

The two sides are implemented in this library: the server and the client.
Server and Client can be instantiate using the factory WsServerFactory or WsClientFactory which allows only one instance.

## Running the server

This is a basic server allowing connection, disconnection, message receiving and sending. When receiving a message, it
just answer this message to the client. A true implemented server can be found [here](https://github.com/jnain/sensapp/tree/master/net.modelbased.sensapp.service.ws)
. This function returns the response to give to this message, of null if the message is not allowed.

* To run the server:

    <pre><code>var port = 9000
    var webSocketServer = WsServerFactory.makeServer(port)
    webSocketServer.start()
    </pre></code>

Now, the server is running and you can connect as much client as you wish.

## Running the client

The client is also a very basic one allowing connection, disconnection, message receiving and sending. A basic "console"
client is implemented [here](https://github.com/jnain/sensapp/tree/master/net.modelbased.sensapp.backyard.echo.ws).

* To run a client:

    <pre><code>var server = "ws://127.0.0.1:9000"
    var serverUri = URI.create(server)
    val client = WsClientFactory.makeClient(serverUri)
    client.connect()
    </pre></code>

## Proof that the connection has been made

If the connection is possible, the client is connecting to the server.
* The server will display:

    <pre><code>New client connected</pre></code>

* And the client will display:

    <pre><code>opened connection</pre></code>

## Sending message

* Now you can use:

    <pre><code>send("MyStringMessage")</pre></code>

from the server to send this message to the client. You can use the same call to send the message from the client to
the server.

* If the message is received on the server, it will display:

    <pre><code>Received Message String: MyStringMessage</pre></code>

* If the client received the message, it will display:

    <pre><code>received: MyStringMessage</pre></code>