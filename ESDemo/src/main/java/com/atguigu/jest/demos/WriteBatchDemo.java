package com.atguigu.jest.demos;

import com.atguigu.jest.beans.Employee;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Delete;
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
public class WriteBatchDemo {

    public static void main(String[] args) throws IOException {

        // ①创建客户端
        JestClientFactory jestClientFactory = new JestClientFactory();

        //为JestClientFactory的HttpClientConfig赋值
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        jestClientFactory.setHttpClientConfig(httpClientConfig);

        JestClient jestClient = jestClientFactory.getObject();

        /*
                向/test/emps插入员工23
                向/test/emps删除员工21
                向/index1/ddd/删除员工2

                将每一个操作都封装为一个Action

         */

        Employee employee = new Employee("1022", "tom", "女", "购物", 2222.33, 20);

        Index index = new Index.Builder(employee)
                .index("test")
                .type("emps")
                .id("21")
                .build();

        Delete delete1 = new Delete.Builder("21").index("test").type("emps").build();
        Delete delete2 = new Delete.Builder("2").index("index1").type("ddd").build();

        // Java语法糖：  类调用方法，之后仍然返回自身
        Bulk bulk = new Bulk.Builder().addAction(index).addAction(delete1).addAction(delete2).build();

        // ③调用客户端的写操作API，向ES服务器发送写请求
        jestClient.execute(bulk);


        // ④关闭客户端
        jestClient.close();


    }
}
