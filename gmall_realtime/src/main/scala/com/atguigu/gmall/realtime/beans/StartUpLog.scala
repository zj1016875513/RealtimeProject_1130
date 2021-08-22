package com.atguigu.gmall.realtime.beans

/**
 * Created by VULCAN on 2021/4/23
 */
case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      `type`:String,
                      vs:String,
                     // 当前启动日志所在的日期
                      var logDate:String,
                     // 当前启动日志所在的日期的小时
                      var logHour:String,
                      var ts:Long)
