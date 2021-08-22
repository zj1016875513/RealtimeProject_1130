package com.atguigu.gmall.realtime.apps

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constansts.GmallConstants
import com.atguigu.gmall.realtime.apps.DAUApp.{appName, internal, streamingContext}
import com.atguigu.gmall.realtime.beans.OrderInfo
import com.atguigu.gmall.realtime.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * Created by VULCAN on 2021/4/26
 *
 *    ①从kafka消费数据，对数据做转换处理，封装为样例类
 *        样例类的属性需要参考kafka中生产的数据的字段数量。
 *          样例类的属性数量 >=  kafka中生产的数据的字段数量
 *
 *          {"payment_way":"1","delivery_address":"fkLWDZljoPkFwsypeBsl","consignee":"SJRioW","create_time":"2021-04-26 05:51:32","order_comment":"dJYVLSfYAJRirjTQyTXi","expire_time":"","order_status":"1","out_trade_no":"9376852471","tracking_no":"","total_amount":"783.0","user_id":"1","img_url":"","province_id":"4","consignee_tel":"13828512714","trade_body":"","id":"5","parent_order_id":"","operate_time":""}
 *         对电话号进行打码
 *    ②将数据写入hbase即可
 *
 *
 *    https://www.cnblogs.com/cailijuan/p/11505217.html
 */
object GMVApp extends  BaseApp {
  override val appName: String = "GMVApp"
  override val internal: Int = 10

  def main(args: Array[String]): Unit = {

    streamingContext=new StreamingContext("local[*]",appName,Seconds(internal))

    runApp{

        //从kafka消费数据
        val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_ORDER_INFO, streamingContext)


      //封装好的样例类
      val ds2: DStream[OrderInfo] = ds.map(record => {

        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //为每个订单生成日期和分时字段   create_time":"2021-04-26 05:51:32"
        val dateTimeFormatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val dateTimeFormatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

        val localDateTime: LocalDateTime = LocalDateTime.parse(orderInfo.create_time,dateTimeFormatter2)

        orderInfo.create_date = localDateTime.format(dateTimeFormatter1)
        orderInfo.create_hour = localDateTime.getHour + ""

        // 为电话号打码
        orderInfo.consignee_tel = orderInfo.consignee_tel.replaceAll("(\\w{3})\\w*(\\w{4})", "$1****$2")

        orderInfo

      })

      ds2.foreachRDD(rdd => {
        rdd.saveToPhoenix("GMALL2020_ORDER_INFO",
          Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
          HBaseConfiguration.create(),
          Some("hadoop102:2181")
        )
      })

    }

  }

}
