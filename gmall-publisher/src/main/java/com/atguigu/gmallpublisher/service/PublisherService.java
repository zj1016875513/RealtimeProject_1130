package com.atguigu.gmallpublisher.service;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.beans.DauPerHour;
import com.atguigu.gmallpublisher.beans.GmvPerHour;

import java.io.IOException;
import java.util.List;

/**
 * Created by VULCAN on 2021/4/24
 */
public interface PublisherService {

    //查询每日的日活
    Integer getDauByDate(String date);

    //查询每日的新增设备数
    Integer getNewMidNumsByDate(String date);

    List<DauPerHour> getRealTimeHoursData(String date);

    //查询每日的GMV
    Double getGmvByDate(String date);


    //查询每日分时的GMV
    List<GmvPerHour> getRealTimeHoursGMVData(String date);

    // 查询订单明细
    JSONObject getOrderDetail(String date, int startpage, int size , String keyword) throws IOException;



}
