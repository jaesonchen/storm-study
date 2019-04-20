package com.asiainfo.storm.wordcount;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.asiainfo.storm.util.ServiceUtil;

/**   
 * @Description: WordCount Topology实例，使用IRichBolt接口，需要自己anchor 新元组，自己调用ack/fail
 * 
 * @author chenzq  
 * @date 2019年4月18日 下午2:23:54
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class WordCountTopology {
    
    public static void main(String[] args) throws Exception {
        new WordCountTopology().run(args);
    }
    
    protected void run(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new SentenceSpout(), 1);
        builder.setBolt("split", new SplitSentence(), 2).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 4).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("print", new PrinterBolt(), 2).globalGrouping("count");
        
        Config conf = new Config();
        String topologyName = "wordcount";
        // 本地运行
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, builder.createTopology());
        // 本地运行一段时间后shutdown，查看cleanup打印结果
        ServiceUtil.sleep(10, TimeUnit.SECONDS);
        cluster.shutdown();
    }
    
    @SuppressWarnings("serial")
    public static class SentenceSpout extends BaseRichSpout {

        private static final String[] INPUTS = {
                "marry had a little lamb whos fleese was white as snow",
                "and every where that marry went the lamb was sure to go",
                "one two three four five six seven eight nine ten",
                "this is a test of the emergency broadcast system this is only a test",
                "peter piper picked a peck of pickeled peppers"};
        
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
            Values tuple = new Values(sentence);
            Object msgId = tuple;
            collector.emit(tuple, msgId);
        }

        // 如果不使用Tuple作为messageId，ack时需要从缓存中删除Tuple
        @Override
        public void ack(Object msgId) {
            super.ack(msgId);
        }

        // 使用缓存(ConcurrentMap、redis等)或者直接使用 Tuple作为messageId
        @Override
        public void fail(Object msgId) {
            super.fail(msgId);
            this.collector.emit((Values) msgId, msgId);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
    }
    
    @SuppressWarnings({"serial", "rawtypes"})
    public static class SplitSentence extends BaseRichBolt {

        OutputCollector collector;
        
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            try {
                String sentence = input.getString(0);
                for (String word : sentence.split("\\s+")) {
                    collector.emit(input, new Values(word));
                }
                collector.ack(input);
            } catch (Exception ex) {
                collector.fail(input);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    @SuppressWarnings({"serial", "rawtypes"})
    public static class WordCount extends BaseRichBolt {
        
        Map<String, Integer> map = new HashMap<String, Integer>();
        OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            try {
                String word = input.getString(0);
                Integer count = map.get(word);
                if (count == null) {
                    count = 0;
                }
                count++;
                map.put(word, count);
                collector.emit(input, new Values(word, count));
                collector.ack(input);
            } catch (Exception ex) {
                collector.fail(input);
            }
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }
    
    @SuppressWarnings({"serial", "rawtypes"})
    public static class PrinterBolt extends BaseRichBolt {

        Map<String, Integer> map = new HashMap<String, Integer>();
        TopologyContext context;
        OutputCollector collector;
        
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
            this.context = context;
        }

        @Override
        public void execute(Tuple input) {
            try {
                String word = input.getStringByField("word");
                Integer count = input.getIntegerByField("count");
                Integer total = map.get(word);
                if (total == null) {
                    total = 0;
                }
                total += count;
                map.put(word, total);
                collector.ack(input);
            } catch (Exception ex) {
                collector.fail(input);
            }
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
