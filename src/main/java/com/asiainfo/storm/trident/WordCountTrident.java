package com.asiainfo.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.asiainfo.storm.util.FixedBatchSpout;
import com.asiainfo.storm.util.MemoryMapState;
import com.asiainfo.storm.util.ServiceUtil;

/**   
 * @Description: wordcount trident 实现
 * 
 * @author chenzq  
 * @date 2019年4月19日 下午4:38:44
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class WordCountTrident {

    public static void main(String[] args) throws Exception {
        
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if (args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
            for (int i = 0; i < 100; i++) {
                //通过drpc调用words方法查询cat/the/dog/jumped在wordcounts的结果中，并返回个数
                System.out.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
                ServiceUtil.sleep(1000);
            }
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(null));
        }
    }
    
    public static StormTopology buildTopology(LocalDRPC drpc) {
        
        // batch spout
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("fuck the cow jumped over the moon"),
            new Values("cnm the man went to the store and bought some candy"), new Values("sb four score and seven years ago"),
            new Values("fuck how many apples can you eat"), new Values("cnm to be or not to be the person"));
        // 设置循环发送
        spout.setCycle(true);

        // 创建TridentTopology实例， 这个对象就是Trident计算的入口。
        TridentTopology topology = new TridentTopology();
        // TridentState 代表了处理结果的统计，用于对外提供给DRPC服务进行查询。
        TridentState wordCounts = topology
            .newStream("spout", spout) // newStream方法在拓扑中创建一个新的数据流以便从输入源(FixedBatchSpout)中读取数据
            .parallelismHint(16) // 设置并行处理的数量
            .each(new Fields("sentence"), new SplitFunction(), new Fields("word")) // 对每个输入的sentence字段，调用Split()函数进行处理，输出word
            .each(new Fields("word"), new SensitiveFilter()) // 对每个输入的word字段进行过滤
            .groupBy(new Fields("word")) // 分组操作，按特定的字段进行分组
            .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")) // 聚合函数，将结果持久到特定的存储介质中
            .parallelismHint(16);

        // 使用同一个TridentTopology对象来创建DRPC流，命名该函数为words，函数名与DRPCClient执行时的第一个参数名称相同。
        // 每个DRPC请求都是一个小的批量作业，将请求的单个tuple作为输入，tuple中包含名为args的字段，args中包含了客户端的请求参数。
        topology
            .newDRPCStream("words", drpc) // 以words作为函数名，对应于drpc.execute("words", "cat the dog jumped")中的words名
            .each(new Fields("args"), new SplitFunction(), new Fields("word")) // 对于输入参数args，使用Split()方法进行切分,并以word作为字段发送
            .groupBy(new Fields("word")) // 对word字段进行重新分区，保证相同的字段落入同一个分区
            .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count")) // 提供对已生成的TridentState对象的查询，输出count
            .each(new Fields("count"), new FilterNull()) // 使用FilterNull()方法过滤count字段的数据(过滤没有统计到的单词)
            .aggregate(new Fields("count"), new Sum(), new Fields("sum")); // 聚合函数，计算总数

        return topology.build();
    }
}
