package com.aditya.github.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

        // Create Producer properties
        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "Jai shree ram");

        // send data - asynchronous
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes every time a record is successfully send or an exception is thrown
                if (e==null){
                    // The record is sent succesfully
                    logger.info("Received new metadata \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " +  recordMetadata.offset() + "\n" +
                            "TimeStamp: " + recordMetadata.timestamp() + "\n");
                }
                else {
                    logger.error("Error while producing " + e);
                }
            }
        });

        //flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }
}