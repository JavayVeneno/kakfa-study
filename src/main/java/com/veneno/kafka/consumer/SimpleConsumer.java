package com.veneno.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class SimpleConsumer {
    private final static String TOPICNAME= "veneno-topic";
    public static void helloWorld(){
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.22.11.222:9092");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"test");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        prop.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(prop);

        consumer.subscribe(Arrays.asList(TOPICNAME));
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s %n",
                        record.partition(),record.offset(),record.key(),record.value());
            }
        }
    }

    public static void commitOffset(){
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.22.11.222:9092");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"test");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        prop.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(prop);

        consumer.subscribe(Arrays.asList(TOPICNAME));
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            int i = 0;
            for (ConsumerRecord<String, String> record : records) {
                try {

                    if(++i%3==0){
                        int x = i/0;
                    }
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s %n",
                            record.partition(),record.offset(),record.key(),record.value());
                    consumer.commitAsync();
                } catch (Exception e) {
                    System.out.println("消费失败"+record.offset()+","+record.partition());
                }

            }
        }
    }    public static void commitOffsetPartition(){
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.22.11.222:9092");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"test");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        prop.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(prop);

        consumer.subscribe(Arrays.asList(TOPICNAME));
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for(TopicPartition partition :records.partitions()){
                List<ConsumerRecord<String, String>> pr = records.records(partition);
                for (ConsumerRecord<String, String> record : pr) {
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s %n",
                            record.partition(),record.offset(),record.key(),record.value());
                }
                //获取最后一次消费的offset
                long lastOffset = pr.get(pr.size()-1).offset();
                Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                map.put(partition,new OffsetAndMetadata(lastOffset+1));
                consumer.commitSync(map);
                System.out.println("=============="+partition+ " END=========");
            }

        }
    }

    public static void main(String[] args) {
//        helloWorld();
//        commitOffset();
        commitOffsetPartition();
    }
}
