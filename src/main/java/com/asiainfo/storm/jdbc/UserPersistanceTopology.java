package com.asiainfo.storm.jdbc;

import java.sql.Types;
import java.util.List;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.TopologyBuilder;

import com.google.common.collect.Lists;

/**   
 * @Description: 使用jdbc 持久化bolt的topology
 * 
 * @author chenzq  
 * @date 2019年4月16日 下午5:32:31
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public class UserPersistanceTopology extends AbstractUserTopology {

    private static final String USER_SPOUT = "USER_SPOUT";
    private static final String LOOKUP_BOLT = "LOOKUP_BOLT";
    private static final String PERSISTANCE_BOLT = "PERSISTANCE_BOLT";
    
    public static void main(String[] args) throws Exception {
        new UserPersistanceTopology().execute(args);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public StormTopology getTopology() {
        JdbcLookupBolt departmentLookupBolt = new JdbcLookupBolt(connectionProvider, SELECT_QUERY, this.jdbcLookupMapper);

        //must specify column schema when providing custom query.
        List<Column> schemaColumns = Lists.newArrayList(new Column("create_date", Types.DATE),
                new Column("dept_name", Types.VARCHAR), new Column("user_id", Types.INTEGER), new Column("user_name", Types.VARCHAR));
        JdbcMapper mapper = new SimpleJdbcMapper(schemaColumns);
        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, mapper)
                .withInsertQuery("insert into user (create_date, dept_name, user_id, user_name) values (?,?,?,?)");

        // userSpout ==> lookupBolt ==> persitenceBolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(USER_SPOUT, this.userSpout, 1);
        builder.setBolt(LOOKUP_BOLT, departmentLookupBolt, 1).shuffleGrouping(USER_SPOUT);
        builder.setBolt(PERSISTANCE_BOLT, userPersistanceBolt, 1).shuffleGrouping(LOOKUP_BOLT);
        return builder.createTopology();
    }
}
