package org.apache.kafka.testclient;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.avro.generic.GenericRecord;
import java.util.Arrays;
import java.util.Properties;

/**
 * Client program to read avro data from a Kafka topic. 
 */
public class KafkaAvroConsumer {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:6667");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    props.put("schema.registry.url", "http://localhost:8081/schemaregistry"); // URL for Schema Registry
    props.put("enable.auto.commit", "true");
    props.put("session.timeout.ms","6000");
    props.put("basic.auth.user.info","admin:test1");
    props.put("basic.auth.credentials.source","USER_INFO");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    String topic = "MyAvroTopic";
    final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
    consumer.subscribe(Arrays.asList(topic));

    try {
      int i=0;
      while (i++ < 5) {
        System.out.println("Poll Attempt#"+i);
        ConsumerRecords<String,GenericRecord> records = consumer.poll(Duration.ofMillis(6000));
        for (ConsumerRecord<String, GenericRecord> record : records) {
          System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
        }
      }
    } finally {
      consumer.close();
    }
  }

}
