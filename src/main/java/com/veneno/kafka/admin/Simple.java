package com.veneno.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.Topic;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Simple {

    private final static String TOPICNAME= "veneno-topic";
    private final static String TOPICNAME2= "jq-topic";

    public static AdminClient create(){
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"10.22.11.222:9092");
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }
    public static void createTopic() throws Exception {
        AdminClient  adminClient = create();
        // 副本因子
        short rs = 1;
        NewTopic topic = new NewTopic(TOPICNAME,1,rs);
        CreateTopicsResult result = adminClient.createTopics(Arrays.asList(topic));
        result.all().get();
        // 获取topic设置的partition数量
        System.out.println(result.numPartitions(TOPICNAME).get());
        System.out.println("result:"+result);

    }
    public static void createTopic2() throws Exception {
        AdminClient  adminClient = create();
        // 副本因子
        short rs = 3;
        NewTopic topic = new NewTopic(TOPICNAME2,3,rs);
        CreateTopicsResult result = adminClient.createTopics(Arrays.asList(topic));
        result.all().get();
        // 获取topic设置的partition数量
        System.out.println(result.numPartitions(TOPICNAME2).get());
        System.out.println("result:"+result);

    }

    public static void getTopicName() throws Exception {
        AdminClient adminClient = create();
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> strings = listTopicsResult.names().get();
        strings.forEach(System.out::println);
    }

    public static void delTopics()throws Exception{
        AdminClient adminClient = create();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPICNAME));
        Thread.sleep(1000);
        deleteTopicsResult.all().get();


    }

    //veneno-topic:
    //(name=veneno-topic,
    // internal=false,
    // partitions=(partition=0, leader=10.22.11.222:9092 (id: 0 rack: null),
    // replicas=10.22.11.222:9092 (id: 0 rack: null), isr=10.22.11.222:9092 (id: 0 rack: null)),
    // authorizedOperations=[])
    public static void descTopic()throws Exception{
        AdminClient adminClient = create();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(TOPICNAME2));

        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        for (Map.Entry<String, TopicDescription> next : stringTopicDescriptionMap.entrySet()) {
            System.out.println(next.getKey() + ":" + next.getValue());
        }

    }

    //ConfigResource(type=TOPIC, name='veneno-topic'):
    // Config(entries=[ConfigEntry(name=compression.type, value=producer, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
    // ConfigEntry(name=leader.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
    // ConfigEntry(name=message.downconversion.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
    // ConfigEntry(name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
    // ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.format.version, value=2.4-IV1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.compaction.lag.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.message.bytes, value=1000012, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])])
    public static void descConfig()throws Exception{
        AdminClient adminClient = create();
        //TODO 集群
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC,TOPICNAME);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singletonList(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
       for( Map.Entry<ConfigResource, Config> next : configResourceConfigMap.entrySet()){
           System.out.println(next.getKey()+":"+next.getValue());
       }
    }
    public static void alterConfig() throws Exception {

        AdminClient adminClient = create();
        Map<ConfigResource,Collection<AlterConfigOp>> map= new HashMap<>();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC,TOPICNAME);
        AlterConfigOp alterConfigOp = new AlterConfigOp(new ConfigEntry("preallocate","true"), AlterConfigOp.OpType.SET);
        map.put(configResource,Arrays.asList(alterConfigOp));
        AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(map);
        alterConfigsResult.all().get();
    }

    public static void incrPartions(int partition) throws Exception {
        AdminClient adminClient = create();
        Map<String,NewPartitions> map = new HashMap<>();
        map.put(TOPICNAME,NewPartitions.increaseTo(partition));
        CreatePartitionsResult partitions = adminClient.createPartitions(map);
        partitions.all().get();

    }

    public static void main(String[] args) throws Exception {
        //创建adminclient
//        AdminClient adminClient = Simple.create();
//        System.out.println(adminClient);
        //创建topic
//        Simple.createTopic();
        //获取topicname
//        getTopicName();
        //删除topic
//        delTopics();
        //获取topic详情
//        descTopic();
        //修改配置
//        alterConfig();
//        //查看配置
//        descConfig();
        //增加partition
//        incrPartions(2);
        //获取topic详情
        descTopic();

//        createTopic2();
    }
}


//jq-topic:(name=jq-topic, internal=false,
//          partitions=(partition=0, leader=10.22.11.222:9092 (id: 0 rack: null),
//                                                      replicas=10.22.11.222:9092 (id: 0 rack: null),
//                                                      10.22.11.222:9093 (id: 1 rack: null),
//                                                      10.22.11.222:9094 (id: 2 rack: null), isr=10.22.11.222:9092 (id: 0 rack: null), 10.22.11.222:9093 (id: 1 rack: null), 10.22.11.222:9094 (id: 2 rack: null)),
//                     (partition=1, leader=10.22.11.222:9094 (id: 2 rack: null),
//                                                      replicas=10.22.11.222:9094 (id: 2 rack: null),
//                                                      10.22.11.222:9092 (id: 0 rack: null),
//                                                      10.22.11.222:9093 (id: 1 rack: null), isr=10.22.11.222:9094 (id: 2 rack: null), 10.22.11.222:9092 (id: 0 rack: null), 10.22.11.222:9093 (id: 1 rack: null)),
//                     (partition=2, leader=10.22.11.222:9093 (id: 1 rack: null),
//                                                      replicas=10.22.11.222:9093 (id: 1 rack: null),
//                                                      10.22.11.222:9094 (id: 2 rack: null),
//                                                      10.22.11.222:9092 (id: 0 rack: null), isr=10.22.11.222:9093 (id: 1 rack: null), 10.22.11.222:9094 (id: 2 rack: null), 10.22.11.222:9092 (id: 0 rack: null)), authorizedOperations=[])