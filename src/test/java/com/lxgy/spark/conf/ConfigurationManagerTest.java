package com.lxgy.spark.conf;

import org.junit.Test;

public class ConfigurationManagerTest {

    @Test
    public void getProperty() {
        String password = ConfigurationManager.getProperty("mysql.passowrd");
    }

}