# SensApp WebSocket Console Echo Client

This is a basic console echo client implementation of a WebSocket client.

It only allow you to connect, disconnect, quit and send messages.

* To connect, use the following:

  <pre><code>connect(ws://127.0.0.1:9000)</code></pre>

* To disconnect:

  <pre><code>disconnect</code></pre>

* To quit:

   <pre><code>quit</code></pre>

* To send a message, just type in your message:

   <pre><code>My String Message</code></pre>


As there is a parsing looking if the line typed is "connect...", "disconnect" or "quit", it is not possible to send
"connect...", "disconnect", "quit" which will be taken as a command line.

Every received message from the server will simply be displayed on the screen.