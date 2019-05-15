package com.asiainfo.storm.redis;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisLookupBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
//import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.common.collect.Lists;

/**   
 * @Description: redis lookup 适用于 String、List、Set、SortedSet、Hash
 * 
 * @author chenzq  
 * @date 2019年4月20日 下午10:21:32
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class RedisLookupTopology {
    
    static final String REDIS_CLUSTER = "192.168.0.102:6010, 192.168.0.102:6020, 192.168.0.102:6030";
    static final String REDIS_HOST = "127.0.0.1";
    static final int REDIS_PORT = 6379;
    
    public static void main(String[] args) throws Exception {
        
        Set<InetSocketAddress> nodes = new HashSet<InetSocketAddress>();
        for (String host : REDIS_CLUSTER.split(",")) {
            String[] host_port = host.split(":");
            nodes.add(new InetSocketAddress(host_port[0].trim(), Integer.valueOf(host_port[1])));
        }
        JedisClusterConfig clusterConfig = new JedisClusterConfig.Builder().setNodes(nodes).build();
        
        //JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
        //        .setHost(REDIS_HOST).setPort(REDIS_PORT).build();

        WordSpout spout = new WordSpout();
        RedisLookupMapper lookupMapper = new WordCountRedisLookupMapper();
        RedisLookupBolt lookupBolt = new RedisLookupBolt(clusterConfig, lookupMapper);
        PrintWordTotalCountBolt printBolt = new PrintWordTotalCountBolt();

        // wordspout -> lookupbolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", spout, 1);
        builder.setBolt("lookup", lookupBolt, 1).shuffleGrouping("spout");
        builder.setBolt("print", printBolt, 1).shuffleGrouping("lookup");
        
        Config conf = new Config();
        String topologyName = "RedisLookupBolt";
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, builder.createTopology());
        System.in.read();
        cluster.shutdown();
    }

    @SuppressWarnings("serial")
    public static class PrintWordTotalCountBolt extends BaseRichBolt {
        
        private static final Random RANDOM = new Random();
        private OutputCollector collector;

        @SuppressWarnings("rawtypes")
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            
            try {
                String wordName = input.getStringByField("wordName");
                String countStr = input.getStringByField("count");
    
                int count = 0;
                // print lookup result with low probability
                if (RANDOM.nextInt(100) > 90) {
                    if (countStr != null) {
                        count = Integer.parseInt(countStr);
                    }
                    System.out.printf("Lookup result - word : %s / %d : ", wordName, count);
                    System.out.println();
                }
                collector.ack(input);
            } catch (Exception ex) {
                // ignore
                collector.fail(input);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }
    
    @SuppressWarnings("serial")
    private static class WordCountRedisLookupMapper implements RedisLookupMapper {
        
        private RedisDataTypeDescription description;
        private final String hashKey = "wordCount";

        public WordCountRedisLookupMapper() {
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.HASH, hashKey);
        }

        @Override
        public List<Values> toTuple(ITuple input, Object value) {
            String key = getKeyFromTuple(input);
            List<Values> values = Lists.newArrayList();
            values.add(new Values(key, value));
            return values;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("wordName", "count"));
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            return tuple.getStringByField("word");
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            return null;
        }
    }
}
