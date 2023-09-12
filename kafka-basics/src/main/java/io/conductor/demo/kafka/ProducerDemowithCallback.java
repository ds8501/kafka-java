package io.conductor.demo.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemowithCallback {
    private static final Logger log=LoggerFactory.getLogger(ProducerDemowithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("hello world");

        Properties properties = new Properties();
        //connected to local host
        //properties.setProperty("bootstrap.servers","127.0.0.1.9092");


        //connected to conductor
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"2OaHFKI0HWz4R8ckIvJAgr\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIyT2FIRktJMEhXejRSOGNrSXZKQWdyIiwib3JnYW5pemF0aW9uSWQiOjc2NjMwLCJ1c2VySWQiOjg5MTU0LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJjNDVhZWEzMS00YjUxLTRhZGUtOGUxYy0zZmQ3MjI5MDc1NmYifX0.kOBEskqpUTjv9kONM62J7h4FbmMFkgRb9EBVfXAa95U\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        //set producer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

       for(int j=0;j<10;j++){
        for (int i = 0; i < 30; i++) {
            // enter record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("java-demo", "hello world" + i);

            //send
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Received new message" + recordMetadata.topic() + "\n"
                                + "Partition" + recordMetadata.partition());
                    } else {
                        log.error("error found");
                    }
                }
            });

        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

        producer.flush();

        producer.close();
    }
}
