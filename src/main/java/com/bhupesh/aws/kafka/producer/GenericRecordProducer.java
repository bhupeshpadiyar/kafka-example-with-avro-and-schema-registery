package com.bhupesh.aws.kafka.producer;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.util.ResourceUtils;

import org.apache.avro.Schema;
import io.confluent.kafka.serializers.KafkaAvroSerializer;



public class GenericRecordProducer {

    private static final String SINGLE_SCHEMA_FILE_CLASS_PATH = "classpath:./schema/transaction-schema-list.json";
    private static final String LIST_SCHEMA_FILE_CLASS_PATH = "classpath:./schema/transaction-schema-list.json";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        produceListSchemaExample(props);
   }

   private static void produceListSchemaExample(Properties props) {

        String schemaDescription = "";
        try {
            File file = ResourceUtils.getFile(LIST_SCHEMA_FILE_CLASS_PATH);
            InputStream inputStream = new FileInputStream(file);
            schemaDescription = IOUtils.toString(inputStream);
        } catch(Exception e) {
            e.printStackTrace();
        }

        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);

        try {

            System.out.println(schemaDescription);
            
            Schema parser = new Schema.Parser().parse(schemaDescription);
            Schema phoneNumberSchema = parser.getTypes().get(0);

            Schema userSchema = parser.getTypes().get(1);

            List<Object> list = new ArrayList<>();

            Date d = new Date();
            GenericRecord genericPhoneNumber = new GenericData.Record(phoneNumberSchema);
            genericPhoneNumber.put("id", Long.valueOf(1L));
            genericPhoneNumber.put("updateDate", d.getTime());
            genericPhoneNumber.put("value", "111-555-5555");
            list.add(genericPhoneNumber);

            genericPhoneNumber = new GenericData.Record(phoneNumberSchema);
            genericPhoneNumber.put("id", Long.valueOf(2L));
            genericPhoneNumber.put("updateDate", d.getTime());
            genericPhoneNumber.put("value", "222-666-6666");
            list.add(genericPhoneNumber);



            GenericRecord genericUser = new GenericData.Record(userSchema);
            genericUser.put("phoneNumbers", list);
            genericUser.put("username", "bhupesh");
            genericUser.put("timestamp", d.getTime());

            ProducerRecord<Object, Object> record = new ProducerRecord<>("users-topic", "key" + d.getTime(), genericUser);
            producer.send(record);

        
        } catch(SerializationException e) {
        // may need to do something with it
            e.printStackTrace();
        }
        // producer.flush();
        producer.close();
   }

   private static void produceSingleSchemaExample(Properties props) {
        String userSchema = "";
        try {
            File file = ResourceUtils.getFile(SINGLE_SCHEMA_FILE_CLASS_PATH);
            InputStream inputStream = new FileInputStream(file);
            userSchema = IOUtils.toString(inputStream);
        } catch(Exception e) {
            e.printStackTrace();
        }

        System.out.println(userSchema);
        

        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);

        String key = "key1";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);

    
        try {
            for(int i=0; i<=10; i++) {

                int randomInt = ThreadLocalRandom.current().nextInt(10, 100);
        
                GenericRecord avroRecord = new GenericData.Record(schema);
                avroRecord.put("f1", "value"+1);
                avroRecord.put("id", ""+i);
                avroRecord.put("amount", randomInt);
                ProducerRecord<Object, Object> record = new ProducerRecord<>("transactions1", key, avroRecord);
                producer.send(record);
            }
        
        } catch(SerializationException e) {
        // may need to do something with it
            e.printStackTrace();
        }
        // producer.flush();
        producer.close();
    }
}
