package com.asiainfo.storm.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaStateUpdater;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**   
 * @Description: trident kafka producer
 * 
 * @author chenzq  
 * @date 2019年4月20日 下午6:18:07
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class TridentKafkaProducerTopology {

    public static void main(String[] args) throws Exception {
        
        Config conf = new Config();
        String topologyName = "toKafka";
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, buildTopology("192.168.0.102:9092", "storm"));
        System.in.read();
        cluster.shutdown();
    }

    @SuppressWarnings("unchecked")
    public static StormTopology buildTopology(String brokerUrl, String topicName) {
        Fields fields = new Fields("word", "count");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 3,
                new Values("storm", "1"),
                new Values("trident", "2"),
                new Values("needs", "3"),
                new Values("javadoc", "4")
        );
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout", spout);

        //set producer properties.
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        TridentKafkaStateFactory<String, String> stateFactory = new TridentKafkaStateFactory<>()
                .withProducerProperties(props)
                .withKafkaTopicSelector(new DefaultTopicSelector(topicName))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("word", "count"));
        stream.partitionPersist(stateFactory, fields, new TridentKafkaStateUpdater<>(), new Fields());
        return topology.build();
    }
}
