package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) {
        String topicName = "SUBSCRIBER";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "subscriber-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (Producer<Integer, String> producer = new KafkaProducer<>(props)) {
            Random random = new Random();
             int sayac=0;
            while (true) {
                int subscId = random.nextInt(1000); // Generate random SUBSC_ID
                String subscName = "Subscriber-" + subscId; // Generate random SUBSC_NAME
                String subscSurname = "Surname-" + subscId; // Generate random SUBSC_SURNAME
                String msisdn = "MSISDN-" + subscId; // Generate random MSISDN

                String message = String.format("%d,%s,%s,%s", subscId, subscName, subscSurname, msisdn);
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, subscId, message);

                producer.send(record);
                System.out.println("Sent record: " + message);

                Thread.sleep(1000);// Wait for one second
                sayac++;
                if(sayac==3) break;

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try (Consumer<Integer, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));


                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Integer, String> record : records) {
                    System.out.println("Received record: " + record.value());
                }

        }
    }
}
/*
* package com.example.kafka.publisher;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class KafkaPublisher {

    public static void main(String[] args) {
        String topicName = "SUBSCRIBER";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<Integer, String> producer = new KafkaProducer<>(props)) {
            Random random = new Random();

            while (true) {
                int subscId = random.nextInt(1000); // Generate random SUBSC_ID
                String subscName = "Subscriber-" + subscId; // Generate random SUBSC_NAME
                String subscSurname = "Surname-" + subscId; // Generate random SUBSC_SURNAME
                String msisdn = "MSISDN-" + subscId; // Generate random MSISDN

                String message = String.format("%d,%s,%s,%s", subscId, subscName, subscSurname, msisdn);
                ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, subscId, message);

                producer.send(record);
                System.out.println("Sent record: " + message);

                Thread.sleep(1000); // Wait for one second
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
*
*
*
*
*
*
*
*
*
*
* package com.example.kafka.subscriber;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaSubscriber {

    public static void main(String[] args) {
        String topicName = "SUBSCRIBER";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "subscriber-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (Consumer<Integer, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));

            while (true) {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Integer, String> record : records) {
                    System.out.println("Received record: " + record.value());
                }
            }
        }
    }
}


* */