package com.veneno.kafka.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class StreamSample {

    public static final String INPUT_TOPIC = "input-stream-topic";
    public static final String OUTPUT_TOPIC = "output-stream-topic";

    public static void stream(){
        Properties prop = new Properties();
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"10.22.11.222:9092");
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG,"word-count-app");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        //构建流结构拓扑
        final StreamsBuilder builder = new StreamsBuilder();
        //构建wordcount process
//        wordcountStream(builder);
        //foreach process
        foreachStream(builder);

        final KafkaStreams streams = new KafkaStreams(builder.build(),prop);
        streams.start();
    }

    private static void foreachStream(StreamsBuilder builder) {
        KStream<String,String> source = builder.stream(INPUT_TOPIC);

        source.flatMapValues(v->Arrays.asList(v.toLowerCase(Locale.getDefault()).split(" ")))
                .foreach((k,v)-> System.out.println(k+":"+v));
    }

    //定义流计算过程
    private static void wordcountStream(StreamsBuilder builder) {
        KStream<String,String> source = builder.stream(INPUT_TOPIC);

        final KTable<String,Long> count = source.flatMapValues(
                value-> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                .groupBy((key,value)->value).count();
        count.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(),Serdes.Long()));

    }

    public static void main(String[] args) {
        stream();
    }
}
