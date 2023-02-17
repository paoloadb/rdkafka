require('dotenv').config()
const Kafka = require('node-rdkafka')
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry')

var consumer = new Kafka.KafkaConsumer({
  'group.id': 'rnd-rdkafka',
  'metadata.broker.list': process.env.KAFKA_BROKER,
  'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'PLAIN',
  'sasl.username': process.env.KAFKA_KEY,
  'sasl.password': process.env.KAFKA_SECRET,
  'compression.codec': 'lz4',
}, {
  'auto.offset.reset': 'earliest' // consume from the start
});

// Define schema registry configuration
const registry = new SchemaRegistry({
  host: process.env.SR_HOST,
  auth: {
    username: process.env.SR_KEY,
    password: process.env.SR_SECRET
  },
})

consumer.connect();

consumer
  .on('ready', function() {
    console.log('Consumer is ready');
    consumer.subscribe(['test_topic']);

    // Consume from the test_topic topic.
    consumer.consume();
  })
  .on('data', async (data) => {
    // Decode the message value
    const decodedMessage = await registry.decode(data.value);
    // Output the actual message contents
    console.log(decodedMessage);
  });

  process.on('SIGINT', () => {
    console.log('SIGINT detected...');
    consumer.disconnect();
    process.exit(0);
  })