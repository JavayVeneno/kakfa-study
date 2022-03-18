package com.veneno.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class SimplePartion implements Partitioner {

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {

        String keyStr = o+"";
        String ketInt = keyStr.substring(4);
        System.out.println("keyStr : "+keyStr+" keyInt : "+ketInt);
        int i =  Integer.parseInt(ketInt);
        return i%2;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
