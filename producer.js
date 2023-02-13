require('dotenv').config()
const Kafka = require('node-rdkafka');

// Define a Kafka producer configuration
const producerConfig = {
  'metadata.broker.list': process.env.KAFKA_BROKER,
  'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'PLAIN',
  'sasl.username': process.env.KAFKA_KEY,
  'sasl.password': process.env.KAFKA_SECRET,
  'linger.ms': 500,
};

// Create a Kafka producer instance
const producer = new Kafka.Producer(producerConfig);

// Connect to the Kafka broker
try {
    producer.connect();
} catch(err) {
    console.log(err);
}

// Wait for the producer to be ready before sending messages
producer.on('ready', () => {
  console.log('Producer is ready');

  // Send a message to the Kafka broker
  producer.produce('test_topic', null, Buffer.from(JSON.stringify({name: 'pao', id: 1})), null);

  // Wait for the message to be delivered and the producer to be flushed before exiting the process
  producer.flush(500, () => {
    console.log('Flushed');
    process.exit(0);
  });
});

// Log any errors that occur
producer.on('error', (err) => {
  console.error('Error from producer:', err);
});

