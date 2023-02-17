require('dotenv').config()
const Kafka = require('node-rdkafka')
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry')

// Define schema registry configuration
const registry = new SchemaRegistry({
  host: process.env.SR_HOST,
  auth: {
    username: process.env.SR_KEY,
    password: process.env.SR_SECRET
  },
})

var cs = new Kafka.KafkaConsumer.createReadStream({
  'group.id': 'rnd-rdkafka',
  'metadata.broker.list': process.env.KAFKA_BROKER,
  'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'PLAIN',
  'sasl.username': process.env.KAFKA_KEY,
  'sasl.password': process.env.KAFKA_SECRET,
}, {
  'auto.offset.reset': 'earliest' // consume from the start
}, {
    topics: ['test_topic']
  });

  cs.consumer.on('ready', () => {
    console.log('Ready');
  })

  cs.on('data', async (message) => {
    console.log('Got message');
    // Decode the message value
    const decodedMessage = await registry.decode(message.value);
    // Output the actual message contents
    console.log(decodedMessage);
  });

  cs.on('error', (err) => {
    if (err) console.log('Stream error', err.message);
    process.exit(1);
  });

  cs.consumer.on('error', (err) => {
    if (err) console.log('Consumer error', err.message);
    process.exit(1);
  });

  process.on('SIGINT', () => {
    console.log('SIGINT detected...');
    process.exit(0);
  })