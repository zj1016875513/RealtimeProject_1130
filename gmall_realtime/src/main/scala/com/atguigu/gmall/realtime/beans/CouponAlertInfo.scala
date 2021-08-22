package com.atguigu.gmall.realtime.beans

/**
 * Created by VULCAN on 2021/4/28
 */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)
