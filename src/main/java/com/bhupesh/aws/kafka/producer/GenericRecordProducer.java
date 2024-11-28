package com.bhupesh.aws.kafka.producer;

import java.util.Properties;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.bhupesh.aws.kafka.model.AvroMessageSerializer;
import com.kafka.message.AvroMessage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import io.confluent.kafka.serializers.KafkaAvroSerializer;



public class GenericRecordProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");


        
        
       // for(int i=0; i==10; i++) {


            KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);

            String key = "key1";
            String userSchema = "{\"type\":\"record\"," +
                                "\"name\":\"myrecord\"," +
                                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(userSchema);

        
            try {
                for(int i=0; i<=10; i++) {
            
                    GenericRecord avroRecord = new GenericData.Record(schema);
                    avroRecord.put("f"+1, "value"+1);
                    ProducerRecord<Object, Object> record = new ProducerRecord<>("topic1", key, avroRecord);
                    producer.send(record);
                }
            
            } catch(SerializationException e) {
            // may need to do something with it
                e.printStackTrace();
            }
            // producer.flush();
            producer.close();
        }
   // }

}
