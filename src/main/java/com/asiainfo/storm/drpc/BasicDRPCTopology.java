package com.asiainfo.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;

/**   
 * @Description: 使用 LinearDRPCTopologyBuilder 构建 drpc topology
 * 
 * @author chenzq  
 * @date 2019年4月20日 下午3:51:51
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class BasicDRPCTopology {

    public static void main(String[] args) throws Exception  {
        
        String topoName = "DRPCExample";
        String function = "exclamation";

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(function);
        builder.addBolt(new ExclaimBolt(), 3);

        Config conf = new Config();
        conf.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createRemoteTopology());

        try (DRPCClient drpc = new DRPCClient(conf, "localhost", 3772)) {
            System.out.println(drpc.execute("exclamation", "aaa"));
            System.out.println(drpc.execute("exclamation", "bbb"));
        }
    }

    @SuppressWarnings("serial")
    public static class ExclaimBolt extends BaseBasicBolt {
        
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String input = tuple.getString(1);
            collector.emit(new Values(tuple.getValue(0), input + "!"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }
    }
}
