# References:

1.https://www.rabbitmq.com/docs/confirms (for nAck/Manual ACK)
2.https://amqp-node.github.io/amqplib/channel_api.html (Also for nACK)
3.https://www.rabbitmq.com/docs/consumers#subscribing (for the RabbitMQ Consumer Pattern)

# Why use push pattern instead of polling

- It works similarly to webSockets, which does not require the application to constantly ask for messages. Instead, application only needs one subscription to the queue through the channel, and receives messages from RabbitMQ whenever there is a new message
- Therefore, it helps on reducing the latency and improves resource efficiency.