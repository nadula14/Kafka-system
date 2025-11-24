const { Kafka } = require('kafkajs');
const avro = require('avsc');
const fs = require('fs');
const path = require('path');
const config = require('../config/kafka');

const schemaPath = path.join(__dirname, '../schemas/order.avsc');
const orderSchema = avro.Type.forSchema(JSON.parse(fs.readFileSync(schemaPath, 'utf8')));

const kafka = new Kafka({
  clientId: config.clientId,
  brokers: config.brokers
});

const producer = kafka.producer();

function generateOrder(orderId) {
  const products = ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard', 'Mouse'];
  return {
    orderId: `ORD-${orderId}`,
    product: products[Math.floor(Math.random() * products.length)],
    price: parseFloat((Math.random() * 1000 + 50).toFixed(2))
  };
}

async function produceOrders() {
  await producer.connect();
  console.log(' Producer connected to Kafka');

  let orderCount = 1;

  setInterval(async () => {
    try {
      const order = generateOrder(orderCount++);
      
      const encodedMessage = orderSchema.toBuffer(order);

      await producer.send({
        topic: config.topics.orders,
        messages: [
          {
            key: order.orderId,
            value: encodedMessage
          }
        ]
      });

      console.log(`📦 Sent: ${order.orderId} - ${order.product} - $${order.price}`);
    } catch (error) {
      console.error(' Error producing message:', error);
    }
  }, 2000); 
}

// Handle shutdown
process.on('SIGINT', async () => {
  console.log('\n Shutting down producer...');
  await producer.disconnect();
  process.exit(0);
});

produceOrders().catch(console.error);