const { Kafka } = require('kafkajs');
const avro = require('avsc');
const fs = require('fs');
const path = require('path');
const config = require('../config/kafka');

// Load Avro schema
const schemaPath = path.join(__dirname, '../schemas/order.avsc');
const orderSchema = avro.Type.forSchema(JSON.parse(fs.readFileSync(schemaPath, 'utf8')));

// Initialize Kafka
const kafka = new Kafka({
  clientId: config.clientId,
  brokers: config.brokers
});

const consumer = kafka.consumer({ groupId: 'order-consumer-group' });
const producer = kafka.producer();

const MAX_RETRIES = 3;
const retryAttempts = new Map();

// Simulate processing with random failures
async function processOrder(order) {
  // Simulate 20% failure rate
  if (Math.random() < 0.2) {
    throw new Error('Temporary processing error');
  }
  
  console.log(` Processed: ${order.orderId} - ${order.product} - $${order.price}`);
  return true;
}

async function sendToRetryTopic(message, attempt) {
  await producer.send({
    topic: config.topics.ordersRetry,
    messages: [
      {
        key: message.key,
        value: message.value,
        headers: {
          'retry-attempt': String(attempt),
          'original-topic': config.topics.orders
        }
      }
    ]
  });
  console.log(`🔄 Sent to retry (attempt ${attempt}): ${message.key.toString()}`);
}

async function sendToDLQ(message, error) {
  await producer.send({
    topic: config.topics.ordersDLQ,
    messages: [
      {
        key: message.key,
        value: message.value,
        headers: {
          'error-message': error.message,
          'failed-at': new Date().toISOString()
        }
      }
    ]
  });
  console.log(`💀 Sent to DLQ: ${message.key.toString()} - Error: ${error.message}`);
}

async function consumeOrders() {
  await consumer.connect();
  await producer.connect();
  console.log('✅ Consumer connected to Kafka');

  await consumer.subscribe({ 
    topics: [config.topics.orders, config.topics.ordersRetry], 
    fromBeginning: false 
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        // Deserialize Avro message
        const order = orderSchema.fromBuffer(message.value);
        const orderId = message.key.toString();

        // Get retry attempt
        const retryHeader = message.headers?.['retry-attempt'];
        const currentAttempt = retryHeader ? parseInt(retryHeader.toString()) : 0;

        try {
          await processOrder(order);
          
          // Clear retry count on success
          retryAttempts.delete(orderId);
        } catch (processingError) {
          const nextAttempt = currentAttempt + 1;

          if (nextAttempt <= MAX_RETRIES) {
            // Send to retry topic
            await sendToRetryTopic(message, nextAttempt);
            retryAttempts.set(orderId, nextAttempt);
          } else {
            // Max retries exceeded, send to DLQ
            await sendToDLQ(message, processingError);
            retryAttempts.delete(orderId);
          }
        }
      } catch (error) {
        console.error(' Error consuming message:', error);
      }
    }
  });
}

// Handle shutdown
process.on('SIGINT', async () => {
  console.log('\n Shutting down consumer...');
  await consumer.disconnect();
  await producer.disconnect();
  process.exit(0);
});

consumeOrders().catch(console.error);