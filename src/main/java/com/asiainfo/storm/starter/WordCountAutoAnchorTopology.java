package com.asiainfo.storm.starter;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.asiainfo.storm.util.ServiceUtil;

/**   
 * @Description: WordCount Topology实例，使用IBasicBolt接口，由BasicOutputCollector自动anchor 新元组，BasicBoltExecutor自动调用ack/fail
 * 
 * @author chenzq  
 * @date 2019年4月18日 下午4:08:50
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class WordCountAutoAnchorTopology {

    public static void main(String[] args) throws Exception {
        new WordCountAutoAnchorTopology().run(args);
    }

    protected void run(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new SentenceSpout(), 1);
        builder.setBolt("split", new SplitSentence(), 2).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 4).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("print", new PrinterBolt(), 2).globalGrouping("count");
        
        String topologyName = "wordcount";
        if (args != null && args.length > 0) {
            topologyName = args[0];
        }
        
        Config conf = new Config();
        conf.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
    }
    
    @SuppressWarnings("serial")
    public static class SentenceSpout extends BaseRichSpout {

        private static final String[] INPUTS = {
                "marry had a little lamb whos fleese was white as snow",
                "and every where that marry went the lamb was sure to go",
                "one two three four five six seven eight nine ten",
                "this is a test of the emergency broadcast system this is only a test",
                "peter piper picked a peck of pickeled peppers"};
        
        Map<String, Values> cache = new ConcurrentHashMap<>();
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
            String sentence = INPUTS[random.nextInt(INPUTS.length)];
            String msgId = UUID.randomUUID().toString();
            Values tuple = new Values(sentence);
            // 缓存Tuple
            cache.put(msgId, tuple);
            collector.emit(tuple, msgId);
        }

        @Override
        public void ack(Object msgId) {
            super.ack(msgId);
            cache.remove(msgId);
        }

        @Override
        public void fail(Object msgId) {
            super.fail(msgId);
            this.collector.emit(cache.get(msgId), msgId);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
    }
    
    @SuppressWarnings("serial")
    public static class SplitSentence extends BaseBasicBolt {

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String sentence = input.getString(0);
            for (String word : sentence.split("\\s+")) {
                collector.emit(new Values(word));
            }
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    @SuppressWarnings("serial")
    public static class WordCount extends BaseBasicBolt {
        
        Map<String, Integer> map = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String word = input.getString(0);
            Integer count = map.get(word);
            if (count == null) {
                count = 0;
            }
            count++;
            map.put(word, count);
            collector.emit(new Values(word, count));
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }
    
    @SuppressWarnings({"serial", "rawtypes"})
    public static class PrinterBolt extends BaseBasicBolt {

        Map<String, Integer> map = new HashMap<String, Integer>();
        TopologyContext context;
        
        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            super.prepare(stormConf, context);
            this.context = context;
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String word = input.getStringByField("word");
            Integer count = input.getIntegerByField("count");
            Integer total = map.get(word);
            if (total == null) {
                total = 0;
            }
            total += count;
            map.put(word, total);
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // 不输出
        }

        @Override
        public void cleanup() {
            super.cleanup();
            System.out.printf("========================port(%d), task(%d), component(%s) cleanup begin==========================", 
                    context.getThisWorkerPort(), context.getThisTaskId(), context.getThisComponentId());
            System.out.println();
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                System.out.printf("%s / %d", entry.getKey(), entry.getValue());
                System.out.println();
            }
            System.out.println("========================cleanup end==========================");
        }
    }
}
