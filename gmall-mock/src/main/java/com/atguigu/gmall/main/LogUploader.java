package com.atguigu.gmall.main;

import java.io.OutputStream;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by VULCAN on 2021/4/21
 *
 *      将造的日志数据，发送到指定的webapp
 *
 *              ①有一个http客户端(类似浏览器)
 *                      HttpURLConnection ： 客户端连接
 *              ② url:
 *                          URL url  =new URL("http://hadoop102/log");
 *              ③ 发请求
 *                          请求方法类型：  GET,POST
 *                                  conn.setRequestMethod("POST"); ： 指定使用post请求
 *
 *              ④ 指定使用什么组件发请求
 *                      <a>: 发get
 *                      <form>: 发get,post
 *
 *                       conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
 *                       指定发送的内容是从一个表单中，将表单填写的内容进行编码后发送
 *
 *              ⑤ 请求参数
 *                          请求参数名=请求参数值
 *
 *                          请求参数名：logString
 *                          请求参数值： 调用sendLogStream，传入的参数
 *
 *
 *          总结：   LogUploader.sendLogStream("abc")
 *                          模拟了一个Http的客户端，开启了一个向  http://hadoop102/log 发送post请求的链接，
 *                          这个连接 会添加  logString="abc" 作为请求参数，添加到一个表单中，将表单中的内容
 *                          发送过去！
 *
 */
public class LogUploader {

    public static void sendLogStream(String log){
        try{
            //不同的日志类型对应不同的URL
            URL url  =new URL("http://localhost:8089/gmall/log");

            //基于一个发送的url，创建一个http的客户端连接
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            //设置请求方式为post
            conn.setRequestMethod("POST");

            //时间头用来供server进行时钟校对的
            conn.setRequestProperty("clientTime",System.currentTimeMillis() + "");

            //允许上传数据
            conn.setDoOutput(true);

            //设置请求的头信息,设置内容类型为JSON
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            System.out.println("upload" + log);

            //输出流
            OutputStream out = conn.getOutputStream();
            out.write(("logString="+log).getBytes());
            out.flush();
            out.close();
            int code = conn.getResponseCode();
            System.out.println(code);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
