package com.atguigu.jest.demos;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * Created by VULCAN on 2021/4/28
 *
 *          JestClient: 客户端对象
 *                 execute(Action action)
 *
 *          Action : 客户端操作对象！
 *              读： Search
 *              批量写： Bulk
 *
 *                         删除： Delete
 *                         插入或更新： Index
 *
 *
 *         在使用ES的Jestapi时，大量使用了建筑者模式！
 *              new  A()
 *              建筑者模式的调用方式：
 *              new  ABulider().setXxx().setXxx().build()
 *
 */
public class WriteDemo1 {

    public static void main(String[] args) throws IOException {

        // ①创建客户端
        JestClientFactory jestClientFactory = new JestClientFactory();

        //为JestClientFactory的HttpClientConfig赋值
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        jestClientFactory.setHttpClientConfig(httpClientConfig);

        JestClient jestClient = jestClientFactory.getObject();

        //  ②封装要写的数据    //向 /test/emps 写  {"empid":1020,"age":20,"balance":2000,"name":"李小三","gender":"男","hobby":"吃饭睡觉"}

        String source="{\"empid\":1020,\"age\":20,\"balance\":2000,\"name\":\"李小三\",\"gender\":\"男\",\"hobby\":\"吃饭睡觉\"}";
        Index index = new Index.Builder(source)
                .index("test")
                .type("emps")
                .id("20")
                .build();

        // ③调用客户端的写操作API，向ES服务器发送写请求
        jestClient.execute(index);


        // ④关闭客户端
        jestClient.close();


    }
}
