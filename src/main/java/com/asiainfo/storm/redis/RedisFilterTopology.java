package com.asiainfo.storm.redis;

import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.redis.bolt.RedisFilterBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisFilterMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;

/**   
 * @Description: redis filter 适用于 String、Set、SortedSet、Hash
 * 
 * @author chenzq  
 * @date 2019年4月20日 下午10:34:12
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class RedisFilterTopology {

    private static final String REDIS_HOST = "127.0.0.1";
    private static final int REDIS_PORT = 6379;
    
    public static void main(String[] args) throws Exception {
        
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost(REDIS_HOST).setPort(REDIS_PORT).build();

        WordSpout spout = new WordSpout();
        RedisFilterMapper filterMapper = new WhitelistWordFilterMapper();
        RedisFilterBolt whitelistBolt = new RedisFilterBolt(poolConfig, filterMapper);
        WordCountBolt wordCounterBolt = new WordCountBolt();
        PrintWordTotalCountBolt printBolt = new PrintWordTotalCountBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", spout, 1);
        builder.setBolt("filter", whitelistBolt, 1).shuffleGrouping("spout");
        builder.setBolt("count", wordCounterBolt, 2).fieldsGrouping("filter", new Fields("word"));
        builder.setBolt("print", printBolt, 1).shuffleGrouping("count");
        
        String topoName = "RedisFilterBolt";
        Config config = new Config();
        StormSubmitter.submitTopology(topoName, config, builder.createTopology());
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
                String word = input.getStringByField("word");
                String countStr = input.getStringByField("count");
    
                // print lookup result with low probability
                if (RANDOM.nextInt(100) > 90) {
                    int count = 0;
                    if (countStr != null) {
                        count = Integer.parseInt(countStr);
                    }
                    System.out.printf("Lookup result - word : %s / %d : ", word, count);
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
    private static class WhitelistWordFilterMapper implements RedisFilterMapper {
        
        private RedisDataTypeDescription description;
        private final String setKey = "whitelist";

        public WhitelistWordFilterMapper() {
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.SET, setKey);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
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
