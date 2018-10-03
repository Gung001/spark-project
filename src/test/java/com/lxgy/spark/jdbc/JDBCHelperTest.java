package com.lxgy.spark.jdbc;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class JDBCHelperTest {

    JDBCHelper jdbcHelper = null;

    @Before
    public void loadJDBChelper() {
        jdbcHelper = JDBCHelper.getInstance();
    }

    @Test
    public void executeUpdate() {
        int executeUpdate = jdbcHelper.executeUpdate("insert into test_user(name,age) values(?,?)",
                new Object[]{"小刚", 23});
        Assert.assertTrue(executeUpdate == 1);
    }

    @Test
    public void executeQuery() {

        final Map<String, Object> testUser = new HashMap<String, Object>();

        jdbcHelper.executeQuery("select * from test_user where id = ? ",
                new Object[]{1},
                new JDBCHelper.QueryCallback() {
                    @Override
                    public void process(ResultSet rs) throws Exception {
                        if (rs.next()) {
                            int age = rs.getInt(1);
                            String name = rs.getString(2);
                            testUser.put("name", name);
                            testUser.put("age", age);
                        }
                    }
                });

        System.out.println("name:" + testUser.get("name") + ", age:" + testUser.get("age"));
    }

    @Test
    public void executeBatch() {

        String sql = "insert into test_user(name,age) values(?,?)";

        List<Object[]> params = new ArrayList<Object[]>();
        params.add(new Object[]{"郭郭", 22});
        params.add(new Object[]{"标标", 22});

        int[] executeBatch = jdbcHelper.executeBatch(sql, params);
        assertTrue(executeBatch.length == 2);
    }
}