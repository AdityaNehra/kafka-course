package com.aditya.github.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKeys {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerKeys.class);

        // Create Producer properties
        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "Hello world "+ Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            // create producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            // send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully send or an exception is thrown
                    if (e == null) {
                        // The record is sent succesfully
                        logger.info("Value: " + value);
                        logger.info("Received new metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "TimeStamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing " + e);
                    }
                }
            });
        }

        //flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }
}