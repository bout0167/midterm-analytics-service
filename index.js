// Import required modules
const express = require('express');  // Express is a minimal Node.js framework for building web applications.
const amqp = require('amqplib/callback_api');  // AMQP (Advanced Message Queuing Protocol) client library for communicating with RabbitMQ.
const cors = require('cors');  // CORS (Cross-Origin Resource Sharing) middleware for handling cross-origin requests.
require('dotenv').config(); // Load .env when present; existing environment variables take precedence.

const app = express();  // Create an Express application instance.
app.use(express.json());  // Middleware to parse incoming JSON request bodies.

// Enable CORS (Cross-Origin Resource Sharing) for all routes
// This allows your API to accept requests from different origins (e.g., your frontend).
app.use(cors());

// Configure the external broker and listening port for this deployment.
const RABBITMQ_CONNECTION_STRING = process.env.RABBITMQ_CONNECTION_STRING || 'amqp://localhost';
const PORT = process.env.PORT || 4000;

const orders = []

//Connect to RabbitMQ and set up a consumer to process incoming orders.
function connectRabbitMQ(){
  const queue = 'order_queue';

  amqp.connect(RABBITMQ_CONNECTION_STRING, (err, conn) => {
    if (err) {
      console.error('connection error:', err);
      return;
    }
    conn.on('error', (error) => {
      console.error(error.message);
    });

    conn.on('close', () => {
      console.log('RabbitMQ connection closed');
    });

    //Create a channel for communication with RabbitMQ
    conn.createChannel((err, channel) => {
      if (err) {
        console.error('Channel error:', err);
        return;
      };

      try {
        // Durable queues survive restarts
        channel.assertQueue(queue, { durable: true }, (err) => {
          if (err) {
            console.error('Queue error:', err);
            return;
          }

          try {
            // Subscribe to the queue and process incoming order messages.
            // Using channel.consume() means that the RabbitMQ Consumer Pattern is Push Consumer, which subscribes to the queue and waits for the messages, thus handles the messages in real-time
            // noAck: false means RabbitMQ waits for the acknowledgement/negative acknowledgement from the consumer, if the consumer channel is closed before sending the acknowledgement, it will requeue the message; 
            // if it receives a negative acknowledgement, the message will be handled by the method channel.nack()
            channel.consume(queue, (msg) => {
              if (msg !== null){
                try{
                  const order = JSON.parse(msg.content.toString());

                  orders.push(order);

                  console.log('order received: ', order);

                  channel.ack(msg);
                }catch(error){
                  console.error('order error: ', error)
                  channel.nack(msg, false, false)
                }
              }
            }, {noAck: false}, (err) => {
              if (err) {
                console.error('Consumer error: ', err);
              }
            });
          } catch (cosumeError) {
            console.error(cosumeError);
            return;
          }
        });
      } catch (queueError) {
        console.error(queueError);
        return;
      }
    }); 
  });
}

// Health Check
app.get('/health', (req, res) => {
  res.json({ status: 'healthy' });
});

// List Orders
app.get('/orders', (req, res) => {
  res.json(orders);
});

app.get('/analytics/summary', (req, res) => {
  const Revenue = orders.reduce((previousSum, currOrder) => previousSum + currOrder.totalPrice, 0);

  let summary ={
    totalOrders : orders.length,
    totalRevenue : Revenue,
    averageOrderValue : orders.length === 0 ? 0 : Revenue / orders.length
  };
  res.json(summary);
})

app.get('/analytics/products', (req, res) => {
  const productNames = []
  const orderCounts = []
  const orderRevenues = []
  const products = []

  for (const order of orders){
    const productName = order.product.name;
    if(!productNames.includes(productName)){
      productNames.push(productName);
      orderCounts.push(1);
      orderRevenues.push(order.totalPrice);
    }
    else{
      orderCounts[productNames.indexOf(productName)] += 1;
      orderRevenues[productNames.indexOf(productName)] += order.totalPrice;
    }
  }

  for (let i = 0; i < productNames.length; i++) {
    let product = {
        productName: productNames[i],
        orderCount: orderCounts[i],
        totalRevenue: orderRevenues[i]
    };

    products.push(product);
}

  res.json(products);
})

app.get('/analytics/top-products', (req, res) => {
  const productNames = [];
  const quantities = [];
  const products = [];

  for (const order of orders) {
    const productName = order.product.name;

    if (!productNames.includes(productName)) {
      productNames.push(productName);
      quantities.push(order.quantity);
    }
    else {
      quantities[productNames.indexOf(productName)] += order.quantity;
    }
  }

  for (let i = 0; i < productNames.length; i++) {
    let product = {
      productName: productNames[i],
      totalQuantity: quantities[i]
    };

    products.push(product);
  }

  products.sort((a, b) => b.totalQuantity - a.totalQuantity);

  res.json(products.slice(0, 3));
})

// Start the server using the configured port.
app.listen(PORT, () => {
  console.log(`Order Analytic Service is running on http://localhost:${PORT}`);
  connectRabbitMQ();
});
