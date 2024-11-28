package com.bhupesh.aws.kafka.producer;

import com.bhupesh.aws.kafka.model.AvroMessageSerializer;
import com.kafka.message.AvroMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;


public class MyKafkaProducerWithAvroSerializer {
public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");

        Producer<Integer, AvroMessage> producer = new KafkaProducer<>(props, new IntegerSerializer(), new AvroMessageSerializer());
        for (int i = 1; i <= 10; i++){
            producer.send(new ProducerRecord<>("myFirstTopic", 0, i, AvroMessage.newBuilder().setId(i).setName(i + "avro value").build()));
        }

        producer.close();
    }

}
