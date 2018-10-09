package com.lxgy.spark.dao.impl;


import com.lxgy.spark.dao.*;

/**
 * DAO工厂类
 *
 * @author Gryant
 */
public class DAOFactory {

    /**
     * 获取任务管理DAO
     *
     * @return DAO
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }

    /**
     * 获取任务管理DAO
     *
     * @return DAO
     */
    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    /**
     * 获取随机抽取DAO
     *
     * @return DAO
     */
    public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new SessionRandomExtractDAOImpl();
    }


    /**
     * 获取随机抽取明细DAO
     *
     * @return DAO
     */
    public static ISessionDetailDAO getSessionDetailDAO() {
        return new SessionDetailDAOImpl();
    }

    /**
     * 获取Top10 DAO
     *
     * @return DAO
     */
    public static ITop10CategoryDAO getTop10CategoryDAO() {
        return new Top10CategoryDAOImpl();
    }

    /**
     * 获取Top10Session DAO
     *
     * @return DAO
     */
    public static ITop10CategorySessionDAO getTop10CategorySessionDAO() {
        return new Top10CategorySessionDAOImpl();
    }


    /**
     * 获取IPageSplitConvertRateDAO
     *
     * @return DAO
     */
    public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
        return new PageSplitConvertRateDAOImpl();
    }



    /**
     * 获取IAreaTop3ProductDAO
     *
     * @return DAO
     */
    public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
        return new AreaTop3ProductDAOImpl();
    }


    /**
     * 获取IAdUserClickCountDAO
     *
     * @return DAO
     */
    public static IAdUserClickCountDAO getAdUserClickCountDAO() {
        return new AdUserClickCountDAOImpl();
    }

    /**
     * 获取IAdBlackListDAO
     *
     * @return DAO
     */
    public static IAdBlackListDAO getAdBlackListDAO() {
        return new AdBlackListDAOImpl();
    }

    /**
     * 获取IAdStatDAO
     *
     * @return DAO
     */
    public static IAdStatDAO getAdStatDAO() {
        return new AdStatDAOImpl();
    }

    /**
     * 获取IAdProvinceTop3DAO
     *
     * @return DAO
     */
    public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {

        return new AdProvinceTop3DAOImpl();
    }


    /**
     * 获取IAdClickTrendDAO
     *
     * @return DAO
     */
    public static IAdClickTrendDAO getAdClickTrendDAO() {
        return new AdClickTrendDAOImpl();
    }

}
