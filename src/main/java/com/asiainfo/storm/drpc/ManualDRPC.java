package com.asiainfo.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;

/**   
 * @Description: 手动构建drpc topology
 * 
 * @author chenzq  
 * @date 2019年4月20日 下午3:47:40
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class ManualDRPC {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        DRPCSpout spout = new DRPCSpout("exclamation");
        builder.setSpout("drpc", spout);
        builder.setBolt("exclaim", new ExclamationBolt(), 3).shuffleGrouping("drpc");
        builder.setBolt("return", new ReturnResults(), 3).shuffleGrouping("exclaim");

        Config conf = new Config();
        StormSubmitter.submitTopology("exclaim", conf, builder.createTopology());
        
        try (DRPCClient drpc = new DRPCClient(conf, "localhost", 3772)) {
            System.out.println(drpc.execute("exclamation", "aaa"));
            System.out.println(drpc.execute("exclamation", "bbb"));
        }
    }
    
    @SuppressWarnings("serial")
    public static class ExclamationBolt extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String arg = tuple.getString(0);
            Object retInfo = tuple.getValue(1);
            collector.emit(new Values(arg + "!!!", retInfo));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("result", "return-info"));
        }
    }
    
}
