package com.atguigu.gmall.realtime.apps

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constansts.GmallConstants
import com.atguigu.gmall.realtime.apps.DAUApp.{appName, internal, streamingContext}
import com.atguigu.gmall.realtime.beans.{CouponAlertInfo, EventLog}
import com.atguigu.gmall.realtime.utils.{MyEsUtil, MyKafkaUtil}
import io.searchbox.client.JestClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
 * Created by VULCAN on 2021/4/28
 *
 * 需求：同一设备(mid)，5分钟内   用不同账号(user_id)登录,  达到 三次及以上  并 每次登录都 领取优惠劵，并且过程中没有浏览商品。
 * 达到以上要求则产生一条预警日志。
 *
 *
 *  输入数据 ---------------计算逻辑 ----------------输出数据
 *
 *  并且同一设备，每分钟只记录一次预警。
 *
 *          数据来源：  用户行为中的事件日志
 *              {"area":"tianjin","uid":"180","itemid":19,"npgid":28,"evid":"addCart","os":"andriod","pgid":38,"
 *              appid":"gmall2021","mid":"mid_275","type":"event","ts":1618989182240}
 *
 *
 *
 *              mid1   userid1   cart_add 28 2021-04- 16：22  ， ts1
 *
 *               mid1   userid1   coupon   2021-04-28 16：24  ,  ts2
 *
 *          预警条件：
 *                    采集的数据的时间范围： 5分钟内     开窗5min
 *
 *                    设备登录的user_id >=3
 *                    且
 *                    每次登录都领取优惠券，     记录sessionid
 *                    且
 *                    没有浏览商品，记录一次预警！
 *
 *
 *          反推：
 *                    采集的数据的时间范围： 5分钟内
 *                    设备登录的user_id < 3
 *                    或
 *                    设备登录的任意一个用户，浏览了商品
 *                    或
 *                    设备登录的任意一个用户，没有领券
 *
 *                    不产生预警日志！
 *
 *         限制： 一个设备每分钟只记录一条预警！
 *                  mid1: 2021-04-28 15:48:01  预警日志1
 *                  mid1: 2021-04-28 15:48:11  预警日志2
 *                  mid1: 2021-04-28 15:48:21  预警日志3
 *
 *                  会只记录其中的一条
 *                    id:  mid+分钟
 *
 *
 *        输出：  预警日志
 *
 *
 */
object AlertApp extends BaseApp {
  override val appName: String = "alertapp"
  override val internal: Int = 10

  def main(args: Array[String]): Unit = {

    runApp{

      streamingContext=new StreamingContext("local[*]",appName,Seconds(internal))

      val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_EVENTS, streamingContext)

      //封装样例类
      val ds1: DStream[EventLog] = ds.map(record => {

        val dateTimeFormatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val dateTimeFormatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")

        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

        //继续为其封装logDate，logHour属性
        val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(eventLog.ts), ZoneId.of("Asia/Shanghai"))

        //基于 ts 封装 日期和小时字段
        eventLog.logDate = localDateTime.format(dateTimeFormatter1)
        eventLog.logHour = localDateTime.format(dateTimeFormatter2)

        eventLog

      })

      ds1.count().print(1000)


      val ds2: DStream[((String, String), Iterable[EventLog])] = ds1
        //开窗计算一个设备最近5分钟的数据
        .window(Minutes(5))
        //将一个设备在5分钟内产生的所有的行为全部分到一个组中，统计登录多少次，领券多少次
        .map(log => ((log.mid, log.uid), log))
        .groupByKey()

      ds2.count.print(1000)


      // 对一个设备 5分钟内登录的同一个用户的事件日志进行统计 ，先过滤5分钟之内，登录用户后，领券的设备
      val ds3: DStream[((String, String), Iterable[EventLog])] = ds2.filter {
        case ((mid, userid), logs) => {
          var ifAlert: Boolean = false
          logs.foreach(log => {
            if (log.evid.equals("coupon")) {
              ifAlert = true
            }
          })
          ifAlert

        }
      }

      /*
          继续分组，按同一个设备分组，将同一设备多个用户的信息合并

            mid_1, userid1, {log1,log2}
            mid_1,userid2 ,  {log3,log4}

            (mid_1,   {  {log1,log2},   {log3,log4}    })
               扁平化
              (mid_1,    {log1,log2,   log3,log4    }）
       */
      val ds4: DStream[(String, Iterable[Iterable[EventLog]])] = ds3.map {
        case ((mid, userid), logs) => (mid, logs)
      }.groupByKey()

      // 5分钟之内以mid分组得到的结果
      val ds5: DStream[(String, Iterable[EventLog])] = ds4.mapValues(iters => iters.flatten)


      //对一个设备 5分钟内登录的同一个用户的事件日志进行统计
      val ds6: DStream[CouponAlertInfo] = ds5.map {
        case (mid,  logs) => {

          //准备需要产生预警日志的字段
          var uids: util.HashSet[String] = new java.util.HashSet[String]()
          var itemIds: util.HashSet[String] = new java.util.HashSet[String]()
          var events: util.List[String] = new java.util.ArrayList[String]()

          //声明变量，是否需要预警
          var ifAlert: Boolean = true

          breakable {
            //进入判断逻辑    目前的需求涉及两种行为 clickItem 和 coupon
            logs.foreach(log => {

              //一旦点击了商品，无需预警
              if (log.evid.equals("clickItem")) {
                ifAlert = false;

                break
              }
              //将每次登录的user_id记录下来
              uids.add(log.uid)
              //记录用户的行为
              events.add(log.evid)
              //如果用户领券，记录用户领券的商品id
              if (log.evid.equals("coupon")) {
                itemIds.add(log.itemid)
              }

            })
          }

          // 统计结束后，根据条件判断是否需要产生预警，如果不需要产生预警，此时返回null
          if (ifAlert == false || uids.size() < 3) {
            null
          } else {
            CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis())
          }
        }
      }

      //ds4.print(1000)

      //去除null值
      val ds7: DStream[CouponAlertInfo] = ds6.filter(_ != null)

      //转换数据
      //必须将数据封装为 List<(Id,Doc)>
      val ds8: DStream[(String, CouponAlertInfo)] = ds7.map(couponAlertInfo => {
        val dateTimeFormatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
        val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(couponAlertInfo.ts), ZoneId.of("Asia/Shanghai"))
        val time: String = localDateTime.format(dateTimeFormatter1)
        (couponAlertInfo.mid + "_" + time, couponAlertInfo)
      })

      //将ds7中的内容写入到es
      ds8.foreachRDD(rdd => {

        rdd.foreachPartition(partition => {

          val client: JestClient = MyEsUtil.getClient

          val indexName="gmall_coupon_alert" + LocalDate.now()

          MyEsUtil.insertBulk(indexName,partition.toList)

          //关闭连接
          MyEsUtil.close(client)

        })

      })

    }

  }

}
