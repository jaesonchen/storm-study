package com.asiainfo.storm.redis;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.asiainfo.storm.util.ServiceUtil;

/**   
 * @Description: TODO
 * 
 * @author chenzq  
 * @date 2019年4月20日 下午10:28:00
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
@SuppressWarnings("serial")
public class WordSpout extends BaseRichSpout {

    public static final String[] words = new String[] { "apple", "orange", "pineapple", "banana", "watermelon" };
    SpoutOutputCollector collector;
    
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        final Random rand = new Random();
        final String word = words[rand.nextInt(words.length)];
        this.collector.emit(new Values(word), UUID.randomUUID());
        ServiceUtil.sleep(100);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
