package com.asiainfo.storm.kafka;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.asiainfo.storm.util.ServiceUtil;


/**   
 * @Description: kafka producer 0.10 以上版本 使用 storm-kafka-client
 * 
 * @author chenzq  
 * @date 2019年4月20日 下午4:42:56
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class KafkaProducerTopology {

    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        String topologyName = "toKafka";
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, buildTopology("192.168.0.102:9092", "storm"));
        System.in.read();
        cluster.shutdown();
    }
    
    public static StormTopology buildTopology(String brokerUrl, String topicName) {
        
        final TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new MessageSpout(), 1);
        
        //set producer properties.
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        KafkaBolt<String, String> bolt = new KafkaBolt<String, String>() // 0.10 以后版本的KafkaBolt使用storm-kafka-client
                // 注入producer属性
                .withProducerProperties(props)
                // 可以自己实现接口 KafkaTopicSelector，用于选择topic
                // DefaultTopicSelector 直接提供topic
                // FieldIndexTopicSelector(index, defalutTopic) topic是Tuple的第index个field，index过大时使用defaultTopic
                // FieldNameTopicSelector(field, defalutTopic) Tuple 存在field时使用field，否则使用defalutTopic
                .withTopicSelector(new DefaultTopicSelector(topicName))
                // 可以自己实现接口 TupleToKafkaMapper，用于转换Tuple -> ProducerRecord
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "message"));
        
        builder.setBolt("forwardToKafka", bolt, 2).shuffleGrouping("spout");
        return builder.createTopology();
    }
    
    @SuppressWarnings("serial")
    public static class MessageSpout extends BaseRichSpout {

        private static final String[] INPUTS = { 
                new String("storm"),
                new String("trident"),
                new String("needs"),
                new String("javadoc") };
        
        SpoutOutputCollector collector;
        Random random;
        
        @SuppressWarnings("rawtypes")
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            this.random = ThreadLocalRandom.current();
        }
        @Override
        public void nextTuple() {
            ServiceUtil.sleep(100);
            String key = INPUTS[random.nextInt(INPUTS.length)];
            String message = UUID.randomUUID().toString();
            Object msgId = message;
            collector.emit(new Values(key, message), msgId);
        }
        
        @Override
        public void ack(Object msgId) {
            super.ack(msgId);
        }
        @Override
        public void fail(Object msgId) {
            super.fail(msgId);
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("key", "message"));
        }
    }
}
