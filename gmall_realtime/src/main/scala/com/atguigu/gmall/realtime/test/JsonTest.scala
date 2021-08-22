package com.atguigu.gmall.realtime.test

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.utils.RedisUtil
import com.google.gson.Gson

/**
 * Created by VULCAN on 2021/4/29
 */
object JsonTest {

  def main(args: Array[String]): Unit = {


    // 使用fastJson的  JSON.toJSON()，写出一个scala对象，必须添加额外的参数 SerializerFeature
    println(JSON.toJSON(Cat("miaomiao")))

    val gson = new Gson()

    println(gson.toJson(Cat("miaomiao")))

    println(RedisUtil.getJedisClient.smembers("aaa"))

  }

}

case class  Cat(var name:String)
