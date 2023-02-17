require('dotenv').config()
const Kafka = require('node-rdkafka');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry')

// Define a Kafka producer configuration
const producerConfig = {
  'metadata.broker.list': process.env.KAFKA_BROKER,
  'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'PLAIN',
  'sasl.username': process.env.KAFKA_KEY,
  'sasl.password': process.env.KAFKA_SECRET,
  'enable.idempotence': true,
  'compression.codec': 'lz4',
  'linger.ms': 500,
};

// Define schema registry configuration
const registry = new SchemaRegistry({
  host: process.env.SR_HOST,
  auth: {
    username: process.env.SR_KEY,
    password: process.env.SR_SECRET
  },
})


// Create a Kafka producer instance
const producer = new Kafka.Producer(producerConfig);

// Connect to the Kafka broker
try {
    producer.connect();
} catch(err) {
    console.log(err);
}

// Wait for the producer to be ready before sending messages
producer.on('ready', async () => {
  console.log('Producer is ready');
// Retrieve schema id and encode message
  const schemaId = await registry.getLatestSchemaId("test_topic-value")
  const encodedMessage = await registry.encode(schemaId, {name: 'pao', id: 1}) 
  // Send a message to the Kafka broker
  producer.produce('test_topic', null, Buffer.from(encodedMessage), null);
  process.exit(0);
});

// Log any errors that occur
producer.on('error', (err) => {
  console.error('Error from producer:', err);
});

process.on('SIGINT', () => {
  console.log('SIGINT detected...');
  // Wait for the internal message queue to be flushed before disconnecting and exiting the process
  producer.flush(500, () => {
    console.log('Flushed');
    process.exit(0);
  });
  producer.disconnect();
  process.exit(0);
})