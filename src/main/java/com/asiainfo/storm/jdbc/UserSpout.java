package com.asiainfo.storm.jdbc;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.asiainfo.storm.util.ServiceUtil;
import com.google.common.collect.Lists;

/**   
 * @Description: Spout 实现，采用模拟的数据源，如果指定messageId，后续的bolt需要通过调用ack或者fail方法反馈给Spout；不传递messageId时，Spout不会跟踪Tuple的异常。
 * 
 * @author chenzq  
 * @date 2019年4月16日 下午4:45:15
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class UserSpout extends BaseRichSpout implements IRichSpout {

    private static final long serialVersionUID = 1L;
    // 模拟输入源
    protected static final List<Values> rows = Lists.newArrayList(
            new Values(1, 1001, "peter", System.currentTimeMillis()),
            new Values(2, 1002, "bob", System.currentTimeMillis()),
            new Values(3, 1003, "alice", System.currentTimeMillis()));
    
    protected SpoutOutputCollector collector;
    
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 从数据源每次读取一个Tuple，Values通常由多个字段，通过emit方法发送到输出流中。
     */
    @Override
    public void nextTuple() {

        ServiceUtil.sleep(1000);
        final Random rand = new Random();
        final Values tuple = rows.get(rand.nextInt(rows.size() - 1));
        this.collector.emit(tuple);
        // 如果指定messageId，后续的bolt需要通过调用ack或者fail方法反馈给Spout；不传递messageId时，Spout不会跟踪tuple的异常。
        //this.collector.emit(tuple, "messageId");
    }

    /**
     * 声明输出流的Tuple 字段顺序、名称
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user_id", "dept_id", "user_name", "create_date"));
    }
}
