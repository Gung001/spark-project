package com.lxgy.spark.dao.impl;

import com.alibaba.fastjson.JSON;
import com.lxgy.spark.dao.ITaskDAO;
import com.lxgy.spark.domain.Task;
import org.junit.Test;

import javax.annotation.Resource;

import static org.junit.Assert.*;

public class DAOFactoryTest {

    @Test
    public void getTaskDAO() {

        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(1);
        System.out.println(JSON.toJSONString(task));

    }

}