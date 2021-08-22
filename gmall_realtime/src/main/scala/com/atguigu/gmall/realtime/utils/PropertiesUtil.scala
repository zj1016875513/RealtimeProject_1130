package com.atguigu.gmall.realtime.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * Created by VULCAN on 2021/4/23
 *
 *  作用读取类路径的指定的  xxx.properties文件，就这个文件的内容封装到一个Properties对象中
 */
object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop=new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }

  def main(args: Array[String]): Unit = {

    val properties: Properties = PropertiesUtil.load("config.properties")

    println(properties.get("redis.port"))


  }

}
