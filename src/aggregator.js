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

const consumer = kafka.consumer({ groupId: 'order-aggregator-group' });

// Aggregation state
let totalOrders = 0;
let totalPrice = 0;
let runningAverage = 0;

async function aggregateOrders() {
  await consumer.connect();
  console.log('✅ Aggregator connected to Kafka');
  console.log('📊 Starting real-time price aggregation...\n');

  await consumer.subscribe({ 
    topic: config.topics.orders, 
    fromBeginning: false 
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        // Deserialize Avro message
        const order = orderSchema.fromBuffer(message.value);

        // Update aggregation
        totalOrders++;
        totalPrice += order.price;
        runningAverage = totalPrice / totalOrders;

        console.log(`📊 Order: ${order.orderId} | Price: $${order.price.toFixed(2)}`);
        console.log(`   Running Average: $${runningAverage.toFixed(2)} (from ${totalOrders} orders)`);
        console.log(`   Total Revenue: $${totalPrice.toFixed(2)}\n`);
      } catch (error) {
        console.error('❌ Error in aggregation:', error);
      }
    }
  });
}

// Handle shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down aggregator...');
  console.log(`\n📈 Final Statistics:`);
  console.log(`   Total Orders: ${totalOrders}`);
  console.log(`   Average Price: $${runningAverage.toFixed(2)}`);
  console.log(`   Total Revenue: $${totalPrice.toFixed(2)}`);
  await consumer.disconnect();
  process.exit(0);
});

aggregateOrders().catch(console.error);