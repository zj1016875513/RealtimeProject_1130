package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.constansts.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by VULCAN on 2021/4/21
 */
@RestController
@Slf4j // Logger log = Logger.getLogger(LogController.class);
public class LogController {

    // 只要引入了springboot继承kafka的starter，自动在容器中为你创建生产者
    @Autowired
    private KafkaTemplate<String,String> producer;

    @RequestMapping(value = "/log")
    public String handle(String logString){

        //收到日志后，生成ts
        //将jsonstr转为 JSONObject
        JSONObject jsonObject = JSON.parseObject(logString);

        //添加时间戳
        jsonObject.put("ts",System.currentTimeMillis());

        //转为jsonstr
        String jsonStr = jsonObject.toJSONString();

        //判断当前收到的是启动日志还是事件日志，不同的日志写入到不同的主题
        if ("startup".equals(jsonObject.getString("type"))){

            producer.send(GmallConstants.GMALL_STARTUP,jsonStr);

        }else{

            producer.send(GmallConstants.GMALL_EVENTS,jsonStr);

        }

       // System.out.println(jsonStr);

        log.info(jsonStr);

        return "success";

    }

}
