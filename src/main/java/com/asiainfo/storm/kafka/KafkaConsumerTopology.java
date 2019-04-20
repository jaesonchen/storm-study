package com.asiainfo.storm.kafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**   
 * @Description: kafka consumer
 * 
 * @author chenzq  
 * @date 2019年4月20日 下午9:04:35
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class KafkaConsumerTopology {

    public static void main(String[] args) throws Exception {

        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(KafkaSpoutConfig.builder("192.168.0.102:9092", "storm").build()), 1);
        // 使用通配符
        //tp.setSpout("kafka_spout", new KafkaSpout<>(KafkaSpoutConfig.builder("192.168.0.102:9092", Pattern.compile("topic.*")).build()), 1);
        tp.setBolt("bolt", new MyMessageBolt()).shuffleGrouping("kafka_spout");
        
        Config conf = new Config();
        //开启topic通配符
        //conf.put("kafka.topic.wildcard.match", true);
        String topologyName = "fromKafka";
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, tp.createTopology());
        System.in.read();
        cluster.shutdown();
    }

    @SuppressWarnings("serial")
    public static class MyMessageBolt extends BaseBasicBolt {

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            System.out.println("====================================");
            System.out.println(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // no output
        }
    }
}
