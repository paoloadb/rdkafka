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
    'retry.backoff.ms': 200,
    'message.send.max.retries': 10,
    'socket.keepalive.enable': true,
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.ms': 1000,
    'batch.num.messages': 1000000,
    'dr_cb': true,
};

// Define schema registry configuration
const registry = new SchemaRegistry({
    host: process.env.SR_HOST,
    auth: {
        username: process.env.SR_KEY,
        password: process.env.SR_SECRET
    },
})

const runProducer = async () => {
    // Creates a producer writable stream
    const stream = Kafka.Producer.createWriteStream(producerConfig, {}, { topic: 'test_topic' });

    // Encodes the message to it's schema registry
    const schemaId = await registry.getLatestSchemaId('test_topic-value')
    const encodedMessage = await registry.encode(schemaId, { id: 1, name: 'stacey' }) 

    // Writes a message to the stream
    const queuedSuccess = stream.write(Buffer.from(encodedMessage));

    if (queuedSuccess) console.log('Successfully queued the message!');
    else console.log('Too many messages in our queue already');

    // Log any errors that occur
    stream.on('error', (err) => {
        console.error('Error from producer:', err);
    });

    process.on('SIGINT', () => {
        console.log('SIGINT detected...');
        stream.destroy();
        process.exit(0);
    });
};

runProducer();
