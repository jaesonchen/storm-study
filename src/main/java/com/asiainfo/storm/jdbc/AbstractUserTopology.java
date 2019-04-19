package com.asiainfo.storm.jdbc;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.tuple.Fields;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**   
 * @Description: Topology 抽象实现
 * 
 * @author chenzq  
 * @date 2019年4月16日 下午5:09:23
 * @version V1.0
 * @Copyright: Copyright(c) 2019 jaesonchen.com Inc. All rights reserved. 
 */
public abstract class AbstractUserTopology {
    
    private static final List<String> setupSqls = Lists.newArrayList(
            "drop table if exists user",
            "drop table if exists department",
            "create table if not exists user (user_id integer, user_name varchar(128), dept_name varchar(128), create_date date)",
            "create table if not exists department (dept_id integer, dept_name varchar(128))",
            "insert into department values (1001, 'R&D')",
            "insert into department values (1002, 'Finance')",
            "insert into department values (1003, 'HR')"
    );
    protected static final String TABLE_NAME = "user";
    protected static final String JDBC_CONF = "jdbc.conf";
    protected static final String SELECT_QUERY = "select dept_name from department where dept_id = ?";
    
    protected UserSpout userSpout;
    protected JdbcMapper jdbcMapper;
    protected JdbcLookupMapper jdbcLookupMapper;
    protected ConnectionProvider connectionProvider;

    @SuppressWarnings("rawtypes")
    public void execute(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: " + this.getClass().getSimpleName() + " [topology name] [dataSource.url]");
            System.exit(-1);
        }
        // 数据库配置
        Map<String, Object> map = Maps.newHashMap();
        initDatabaseConfig(map, args);

        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(map);
        connectionProvider.prepare();
        int queryTimeoutSecs = 60;
        JdbcClient jdbcClient = new JdbcClient(connectionProvider, queryTimeoutSecs);
        // 初始化数据库便于测试
        for (String sql : setupSqls) {
            jdbcClient.executeSql(sql);
        }

        this.userSpout = new UserSpout();
        this.connectionProvider = new HikariCPConnectionProvider(map);
        this.jdbcMapper = new SimpleJdbcMapper(TABLE_NAME, connectionProvider);
        connectionProvider.cleanup();
        
        Fields outputFields = new Fields("user_id", "user_name", "dept_name", "create_date");
        List<Column> queryParamColumns = Lists.newArrayList(new Column("dept_id", Types.INTEGER));
        this.jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
        this.connectionProvider = new HikariCPConnectionProvider(map);
        
        Config config = new Config();
        config.put(JDBC_CONF, map);
        String topoName = args[0];
        StormSubmitter.submitTopology(topoName, config, getTopology());
    }

    private void initDatabaseConfig(Map<String, Object> map, String[] args) {
        map.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        map.put("dataSource.url", args.length > 1 ? args[1] : "jdbc:mysql://127.0.0.1:3306/storm");
        map.put("dataSource.user", "root");
        map.put("dataSource.password", "root");
    }
    
    public abstract StormTopology getTopology();
}
