package io.conductor.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemowithShutDown {

        private static final Logger log= LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
        public static void main(String[] args) {
            log.info("This is consumer!!");

            String groupId = "my-java-application";
            String topic = "java-demo";
            Properties properties = new Properties();
            //connected to local host
            //properties.setProperty("bootstrap.servers","127.0.0.1.9092");


            //connected to conductor
            properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
            properties.setProperty("security.protocol", "SASL_SSL");
            properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"2OaHFKI0HWz4R8ckIvJAgr\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIyT2FIRktJMEhXejRSOGNrSXZKQWdyIiwib3JnYW5pemF0aW9uSWQiOjc2NjMwLCJ1c2VySWQiOjg5MTU0LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJjNDVhZWEzMS00YjUxLTRhZGUtOGUxYy0zZmQ3MjI5MDc1NmYifX0.kOBEskqpUTjv9kONM62J7h4FbmMFkgRb9EBVfXAa95U\";");
            properties.setProperty("sasl.mechanism", "PLAIN");


            properties.setProperty("key.deserializer", StringDeserializer.class.getName());
            properties.setProperty("value.deserializer", StringDeserializer.class.getName());

            properties.setProperty("group.id", groupId);
            properties.setProperty("auto.offset.reset", "earliest");

            //set Consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

            final Thread mainThread=Thread.currentThread();

            //adding shutdown hook

            Runtime.getRuntime().addShutdownHook(new Thread(){
                public void run(){
                    log.info("Detected shutdown ");
                    consumer.wakeup();

                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
           try {
               consumer.subscribe(Arrays.asList(topic));

               while (true) {
                   log.info("Polling");
                   ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                   for (ConsumerRecord<String, String> record : records) {
                       log.info("key :" + record.key() + "\n" + "Value:" + record.value());
                       log.info("Partiton :" + record.partition() + "\n" + "Offset:" + record.offset());
                   }
               }
           }catch (WakeupException e){
               log.info("Consumer started to shutdown ");
           }catch (Exception e){
               log.info("Unexpected Exception");
           }finally {
               consumer.close();
               log.info("Consumer is shutdown");
           }
        }
}
