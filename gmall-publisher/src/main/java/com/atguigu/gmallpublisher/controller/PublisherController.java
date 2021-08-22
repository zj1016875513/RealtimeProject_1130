package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.beans.DauPerHour;
import com.atguigu.gmallpublisher.beans.GmvPerHour;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by VULCAN on 2021/4/24
 *
 * http://localhost:8070/realtime-total?date=2020-08-15
 *
 *     [
 *      {"id":"dau","name":"新增日活","value":1200},
 *     {"id":"new_mid","name":"新增设备","value":233},
 *     {"id":"order_amount","name":"新增交易额","value":1000.2 }
 *     ]
 *
 *     [] :  Arraylist ,JSONArray
 *     {} : Map,JSONObject
 *
 *      请求每天的日活
 * http://localhost:8070/realtime-hours?id=dau&date=2020-08-15
 *      {
 *      "yesterday":{"11":383,"12":123,"17":88,"19":200 },
 *       "today":{"12":38,"13":1233,"17":123,"19":688 }
 *       }
 *
 *       请求每天的GMV
 *       http://localhost:8070/realtime-hours?id=order_amount&date=2020-08-15
 *
 */
@RestController //只返回数据
public class PublisherController {

    @Autowired
    private PublisherService publisherService;


    // http://localhost:8070/sale_detail?date=2020-08-21&startpage=1&size=5&keyword=小米手机
    @RequestMapping(value = "/sale_detail")
    public JSONObject handle3(String date,int startpage,int size ,String keyword) throws IOException {

        return publisherService.getOrderDetail(date,startpage,size,keyword);
    }

    @RequestMapping(value = "/realtime-total")
    public ArrayList<JSONObject> handle1(String date){

        Integer dauByDate = publisherService.getDauByDate(date);
        Integer newMidNumsByDate = publisherService.getNewMidNumsByDate(date);
        Double gmvByDate = publisherService.getGmvByDate(date);

        ArrayList<JSONObject> result = new ArrayList<>();

        JSONObject jsonObject1 = new JSONObject();
        JSONObject jsonObject2 = new JSONObject();
        JSONObject jsonObject3 = new JSONObject();

        jsonObject1.put("id","dau");
        jsonObject1.put("name","新增日活");
        jsonObject1.put("value",dauByDate);

        jsonObject2.put("id","new_mid");
        jsonObject2.put("name","新增设备");
        jsonObject2.put("value",newMidNumsByDate);

        jsonObject3.put("id","order_amount");
        jsonObject3.put("name","新增交易额");
        jsonObject3.put("value",gmvByDate);

        result.add(jsonObject1);
        result.add(jsonObject2);
        result.add(jsonObject3);

        // 返回的是对象，spring自动调用jackson将对象转为jsonstr再返回
        return result;
    }

    @RequestMapping(value = "/realtime-hours")
    public Object handle2(String date,String id){

        //获取传入日期的昨天七日   string 转   LocalDate
        String yesToday = LocalDate.parse(date).minusDays(1).toString();

        JSONObject result = new JSONObject();

        if (id.equals("dau")){

            JSONObject todayData = parseDauPerHours(publisherService.getRealTimeHoursData(date));
            JSONObject yestodayData = parseDauPerHours(publisherService.getRealTimeHoursData(yesToday));

            result.put("yesterday",yestodayData);
            result.put("today",todayData);

            return result;
        }else {

            JSONObject todayData = parseGmvPerHours(publisherService.getRealTimeHoursGMVData(date));
            JSONObject yestodayData = parseGmvPerHours(publisherService.getRealTimeHoursGMVData(yesToday));

            result.put("yesterday",yestodayData);
            result.put("today",todayData);

            return result;

        }


    }

    public  JSONObject parseDauPerHours(List<DauPerHour> datas){

        JSONObject result = new JSONObject();

        for (DauPerHour data : datas) {
            result.put(data.getHour(),data.getDauNums());
        }

        return result;

    }

    public  JSONObject parseGmvPerHours(List<GmvPerHour> datas){

        JSONObject result = new JSONObject();

        for (GmvPerHour data : datas) {
            result.put(data.getHour(),data.getGmv());
        }

        return result;

    }

}
