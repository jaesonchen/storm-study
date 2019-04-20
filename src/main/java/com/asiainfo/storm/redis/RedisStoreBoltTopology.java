package com.asiainfo.storm.redis;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;

/**   
 * @Description: redis store
 * 
 * @author chenzq  
 * @date 2019年4月20日 下午10:46:07
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class RedisStoreBoltTopology {

    private static final String REDIS_HOST = "127.0.0.1";
    private static final int REDIS_PORT = 6379;
    
    public static void main(String[] args) throws Exception {

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost(REDIS_HOST).setPort(REDIS_PORT).build();
        
        WordSpout spout = new WordSpout();
        WordCounter bolt = new WordCounter();
        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);

        // wordSpout ==> countBolt ==> storeBolt 
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", spout, 1);
        builder.setBolt("count", bolt, 2).fieldsGrouping("spout", new Fields("word"));
        builder.setBolt("store", storeBolt, 1).shuffleGrouping("count");
        
        String topoName = "RedisStoreBolt";
        Config config = new Config();
        StormSubmitter.submitTopology(topoName, config, builder.createTopology());
    }

    @SuppressWarnings("serial")
    private static class WordCountStoreMapper implements RedisStoreMapper {
        
        private RedisDataTypeDescription description;
        private final String hashKey = "wordCount";

        public WordCountStoreMapper() {
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.HASH, hashKey);
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
            return tuple.getStringByField("count");
        }
    }
}
