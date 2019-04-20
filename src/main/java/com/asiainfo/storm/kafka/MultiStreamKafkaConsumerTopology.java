package com.asiainfo.storm.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**   
 * @Description: trident 不支持多个 stream
 * 
 * @author chenzq  
 * @date 2019年4月20日 下午9:21:43
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class MultiStreamKafkaConsumerTopology {

    public static void main(String[] args) throws Exception {
        
        Config conf = new Config();
        String topologyName = "fromKafka";
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, buildTopology("192.168.0.102:9092"));
        System.in.read();
        cluster.shutdown();
    }
    
    public static StormTopology buildTopology(String brokerUrl) {
        
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(kafkaSpoutConfig(brokerUrl)), 1);
        tp.setBolt("bolt", new MyMessageBolt("STREAM_1")).shuffleGrouping("kafka_spout", "STREAM_1");
        tp.setBolt("another", new MyMessageBolt("STREAM_2")).shuffleGrouping("kafka_spout", "STREAM_2");
        return tp.createTopology();
    }
    
    protected static KafkaSpoutConfig<String, String> kafkaSpoutConfig(String brokerUrl) {
        
        //默认情况下,KafkaSpout 消费但未被match到的topic的message将发送到"STREAM_1"
        ByTopicRecordTranslator<String, String> translator = new ByTopicRecordTranslator<>(
                (r) -> new Values(r.topic(), r.key(), r.value()), 
                new Fields("topic", "key", "value"), "STREAM_1");
        //topic_2 所有的消息将发送到 "STREAM_2"中
        translator.forTopic("storm", 
                (r) -> new Values(r.topic(), r.offset(), r.key(), r.value()), 
                new Fields("offset", "topic", "key", "value"), "STREAM_2");
        
        return KafkaSpoutConfig.builder(brokerUrl, new String[] { "com.asiainfo.request", "storm" })
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutGroup")
            // 可以自己实现接口RecordTranslator转换 CosumerRecord -> Tuple
            .setRecordTranslator(translator)
            // offset起始策略：
            // EARLIEST 无论之前的消费情况如何，spout会从每个partition能找到的最早的offset开始的读取
            // LATEST   无论之前的消费情况如何，spout会从每个partition当前最新的offset开始的读取
            // UNCOMMITTED_EARLIEST (默认值)  spout 会从每个partition的最后一次提交的offset开始读取；如果offset不存在或者过期，则会依照 EARLIEST进行读取。
            // UNCOMMITTED_LATEST spout 会从每个partition的最后一次提交的offset开始读取，如果offset不存在或者过期，则会依照 LATEST进行读取。
            .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.EARLIEST)
            .setOffsetCommitPeriodMs(10_000)
            .setMaxUncommittedOffsets(250)
            .build();
    }
    
    @SuppressWarnings("serial")
    public static class MyMessageBolt extends BaseBasicBolt {

        String streamId = "default";
        public MyMessageBolt() {
            
        }
        
        public MyMessageBolt(String streamId) {
            this.streamId = streamId;
        }
        
        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            System.out.printf("================={%s}===================", this.streamId);
            System.out.println();
            System.out.println(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // no output
        }
    }
}
