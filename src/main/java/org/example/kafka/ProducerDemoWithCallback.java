package org.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

  private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

  public static void main(String[] args) {
    log.info("I am a Kafka producer");

    // create Producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the Producer
    KafkaProducer<String, String> producer = new KafkaProducer(properties);

    // create a producer record
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demoJava", "Hello World");

    // send the data - asynchronous
    producer.send(
        producerRecord,
        new Callback() {
          @Override
          public void onCompletion(RecordMetadata metadata, Exception exception) {
            // executes every time a record is successfully sent or on exception is thrown
            if (exception == null) {
              // the record was successfully sent
              log.info(
                  "Received new metadata \n"
                      + "Topic "
                      + metadata.topic()
                      + "\n"
                      + "Partition: "
                      + metadata.partition()
                      + "\n"
                      + "Offset: "
                      + metadata.offset()
                      + "\n"
                      + "Timestamp: "
                      + metadata.timestamp());
            } else {
              log.error("Error while producing", exception);
            }
          }
        });

    // flush data - synchronous
    producer.flush();

    //flush and close the producer
    producer.close();
  }
}
