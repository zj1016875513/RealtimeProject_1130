package com.atguigu.gmall.realtime.test

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Date

/**
 * Created by VULCAN on 2021/4/23
 *    SimpleDateFormat:  要格式化的时间的样式  DateTimeFormatter
 *    Date：  代表一个时间日期对象    LocalDateTime
 *
 */
object TimeParseTest {

  def main(args: Array[String]): Unit = {

    //基于一个时间戳，获取这个时间戳所对应的 日期(yyyy-MM-dd) 和 小时(hh)

    val simpleDateFormat1 = new SimpleDateFormat("yyyy-MM-dd")
    val simpleDateFormat2 = new SimpleDateFormat("HH")

    //时间戳转换的日期对象
    val date = new Date()

    println(simpleDateFormat1.format(date))
    println(simpleDateFormat2.format(date))

    println("--------------------java.time包下有新的api-----------------------")

    val dateTimeFormatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val dateTimeFormatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")


    val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.of("Asia/Shanghai"))

    println(localDateTime.format(dateTimeFormatter1))
    println(localDateTime.format(dateTimeFormatter2))

    println(LocalDate.now())



  }

}
