package com.atguigu.gmall.realtime.apps

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.constansts.GmallConstants
import com.atguigu.gmall.realtime.apps.DAUApp.{appName, internal, streamingContext}
import com.atguigu.gmall.realtime.beans.UserInfo
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * Created by VULCAN on 2021/4/29
 */
object UserInfoApp extends BaseApp {
  override val appName: String = "UserInfoApp"
  override val internal: Int = 5

  def main(args: Array[String]): Unit = {

    runApp{

      streamingContext=new StreamingContext("local[*]",appName,Seconds(internal))

      val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_USER_INFO, streamingContext)

      //{"birthday":"1991-04-30","login_name":"NJSuxaKxkFwqqHovndDJ","gender":"F",
      // "create_time":"2021-04-29 19:14:49","passwd":"pwdUHBLZRjvBsldIgJOaLBz",
      // "nick_name":"wqmnFloLOKkTQvFOcYKF","head_img":"http://bPTXiZwlPqiUYjXozEAPxSYmnJtXFloLOLnfQMma",
      // "name":"VbxkFyBnHyvHYXSxHUCcJZahHQjuxe","user_level":"2","phone_num":"13095720242","id":"1","email":"TJQdQXoA@KYP.com"}
      val result: DStream[JSONObject] = ds.map(record => {

        val originalJO: JSONObject = JSON.parseObject(record.value())

        val result = new JSONObject()

        result.put("id", originalJO.getString("id"));
        result.put("login_name", originalJO.getString("login_name"));
        result.put("user_level", originalJO.getString("user_level"));
        result.put("gender", originalJO.getString("gender"));
        result.put("birthday", originalJO.getString("birthday"));

        result

      })

      result.foreachRDD(rdd => {

        rdd.foreachPartition(partition => {

          val jedisClient: Jedis = RedisUtil.getJedisClient

          partition.foreach(userJO => {

            // Key:  user_+id
            jedisClient.set("user_"+userJO.getString("id"),userJO.toJSONString)

          })

          jedisClient.close()

        })

      })


    }


  }
}
