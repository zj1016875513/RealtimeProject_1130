package com.atguigu.gmall.realtime.beans

/**
 * Created by VULCAN on 2021/4/28
 */
case class EventLog(mid:String,
                    uid:String,
                    appid:String,
                    area:String,
                    os:String,
                    `type`:String,
                    evid:String,
                    pgid:String,
                    npgid:String,
                    itemid:String,
                   // 额外封装
                    var logDate:String,
                    var logHour:String,
                    var ts:Long)
