package com.veneno.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerSimple {
    private final static String TOPICNAME= "veneno-topic";

    public static void main(String[] args) throws Exception {
//        send();
        sendWithCallBackLoadBalancePartition();
    }

    public static void send() throws Exception {
        Properties properties = new Properties();
        //地址
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.22.11.222:9092");
        //确认机制
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        //重试机制
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        //批量数
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        //
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        //缓冲大小
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        // 序列化
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPICNAME, "key" + i, "value" + i);
            Future<RecordMetadata> send = producer.send(record);
            send.get();
        }
    }

        public static void sendWithCallBackLoadBalancePartition() throws Exception {
            Properties properties = new Properties();
            //地址
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.22.11.222:9092");
            //确认机制
            properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
            //重试机制
            properties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
            //批量数
            properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
            //
            properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
            //缓冲大小
            properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

            // 序列化
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.veneno.kafka.producer.SimplePartion");
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

            for (int i = 0; i < 100; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPICNAME, "key-" + i, "value" + i);
                Future<RecordMetadata> send = producer.send(record, (recordMetadata, e) -> {
                    System.out.println("partition : " + recordMetadata.partition() + ", offset : " + recordMetadata.offset());
                });
                send.get();
            }
        }
}
