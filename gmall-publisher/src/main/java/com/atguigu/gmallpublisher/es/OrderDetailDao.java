package com.atguigu.gmallpublisher.es;

import com.alibaba.fastjson.JSONObject;

import java.io.IOException;

/**
 * Created by VULCAN on 2021/4/29
 */
public interface OrderDetailDao {

    // 查询订单明细
    JSONObject getOrderDetail(String date, int startpage, int size , String keyword) throws IOException;

}
