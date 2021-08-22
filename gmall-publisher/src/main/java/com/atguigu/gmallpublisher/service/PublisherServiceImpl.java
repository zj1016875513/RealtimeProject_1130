package com.atguigu.gmallpublisher.service;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.beans.DauPerHour;
import com.atguigu.gmallpublisher.beans.GmvPerHour;
import com.atguigu.gmallpublisher.dao.DauMapper;
import com.atguigu.gmallpublisher.dao.GmvMapper;
import com.atguigu.gmallpublisher.es.OrderDetailDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * Created by VULCAN on 2021/4/24
 */
@Service //new PublisherServiceImpl
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired //  gmvMapper=0xxxx
    private GmvMapper gmvMapper;

    @Autowired
    private OrderDetailDao orderDetailDao;

    @Override
    public Integer getDauByDate(String date) {

        //复杂的业务逻辑

        return dauMapper.getDauByDate(date);
    }

    @Override
    public Integer getNewMidNumsByDate(String date) {

        //复杂的业务逻辑

        return dauMapper.getNewMidNumsByDate(date);
    }

    @Override
    public List<DauPerHour> getRealTimeHoursData(String date) {

        //复杂的业务逻辑
        return dauMapper.getRealTimeHoursData(date);
    }

    @Override
    public Double getGmvByDate(String date) {
        return gmvMapper.getGmvByDate(date);
    }

    @Override
    public List<GmvPerHour> getRealTimeHoursGMVData(String date) {
        return gmvMapper.getRealTimeHoursData(date);
    }

    @Override
    public JSONObject getOrderDetail(String date, int startpage, int size, String keyword) throws IOException {
        return orderDetailDao.getOrderDetail(date,startpage,size,keyword);
    }
}
