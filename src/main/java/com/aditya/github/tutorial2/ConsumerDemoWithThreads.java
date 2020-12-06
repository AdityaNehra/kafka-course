package com.aditya.github.tutorial2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.omg.SendingContext.RunTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    public ConsumerDemoWithThreads() {

    }

    public void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        CountDownLatch latch = new CountDownLatch(1);
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my_sixth_application";
        String topic = "first_topic";

        logger.info("Creating a consumer thread.");
        Runnable myConsumerRunnable  = new ConsumerRunnable(bootstrapServer, groupId, topic, latch);
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();


        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            latch.await();
        }catch (InterruptedException e){
            logger.error("Application got interrupted", e);
        }finally{
            logger.info("Application is closing");
        }

    }


    public class ConsumerRunnable implements Runnable{
        private Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch ){
            this.latch = latch;
            //Create Consumer Config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //create a consumer
            consumer = new KafkaConsumer<String, String>(properties);
            //Subscribe consumer to our topic
            consumer.subscribe(Arrays.asList(topic));
        }


        @Override
        public void run() {
            try {
                //pull for new data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord record : records) {
                        logger.info("Key: " + record.key() + "\n");
                        logger.info(",Value: " + record.value() + "\n");
                        logger.info(",Partition: " + record.partition() + "\n");
                        logger.info(",Offset: " + record.offset() + "\n");
                    }
                }
            }
            catch (WakeupException e){
                logger.info("Info received shutdown signal.");
            }
            finally {
                consumer.close();
                //tell our main code that we are down with the consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw a WakeUpException
            consumer.wakeup();
        }
    }
}
