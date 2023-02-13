require('dotenv').config()
const Kafka = require('node-rdkafka')

var consumer = new Kafka.KafkaConsumer({
  'group.id': 'rnd-rdkafka',
  'metadata.broker.list': process.env.KAFKA_BROKER,
  'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'PLAIN',
  'sasl.username': process.env.KAFKA_KEY,
  'sasl.password': process.env.KAFKA_SECRET,
}, {
  'auto.offset.reset': 'earliest' // consume from the start
});

consumer.connect();

consumer
  .on('ready', function() {
    console.log('Consumer is ready');
    consumer.subscribe(['test_topic']);

    // Consume from the librdtesting-01 topic. This is what determines
    // the mode we are running in. By not specifying a callback (or specifying
    // only a callback) we get messages as soon as they are available.
    consumer.consume();
  })
  .on('data', function(data) {
    // Output the actual message contents
    console.log(data.value.toString());
  });

  process.on('SIGINT', () => {
    console.log('SIGINT detected...');
    consumer.disconnect();
    process.exit(0);
  })