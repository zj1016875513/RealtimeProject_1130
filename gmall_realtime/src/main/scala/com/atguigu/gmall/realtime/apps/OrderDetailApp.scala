package com.atguigu.gmall.realtime.apps

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constansts.GmallConstants
import com.atguigu.gmall.realtime.apps.DAUApp.streamingContext
import com.atguigu.gmall.realtime.apps.GMVApp.{appName, internal, streamingContext}
import com.atguigu.gmall.realtime.beans.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall.realtime.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
import com.google.gson.Gson
import io.searchbox.client.JestClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * Created by VULCAN on 2021/4/29
 *
 *    双流Join的要求：
 *          ①两个流必须从同一个StreamingContext中获取
 *          ②只有DS[K-V]才能Join
 *
 *          orderDetail left join  orderInfo   on orderDetail.orderId = orderInfo.id
 */
object OrderDetailApp extends  BaseApp {
  override val appName: String = "OrderDetailApp"
  override val internal: Int = 5

  def main(args: Array[String]): Unit = {

    runApp{

      streamingContext=new StreamingContext("local[*]",appName,Seconds(internal))

      val ds1: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_ORDER_DETAIL, streamingContext)
      val ds2: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_ORDER_INFO, streamingContext)

      val ds3: DStream[(String, OrderDetail)] = ds1.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })

      val ds4: DStream[(String, OrderInfo)] = ds2.map(record => {

        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //为每个订单生成日期和分时字段   create_time":"2021-04-26 05:51:32"
        val dateTimeFormatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val dateTimeFormatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

        val localDateTime: LocalDateTime = LocalDateTime.parse(orderInfo.create_time, dateTimeFormatter2)

        orderInfo.create_date = localDateTime.format(dateTimeFormatter1)
        orderInfo.create_hour = localDateTime.getHour + ""

        // 为电话号打码
        orderInfo.consignee_tel = orderInfo.consignee_tel.replaceAll("(\\w{3})\\w*(\\w{4})", "$1****$2")

        (orderInfo.id, orderInfo)

      })

      val ds5: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = ds4.fullOuterJoin(ds3)


      // 对join后的数据进行处理
      val joinedDatas: DStream[SaleDetail] = ds5.mapPartitions(partition => {

        val jedisClient: Jedis = RedisUtil.getJedisClient

        val gson = new Gson()

        //存放已经封装好的销售详情的集合
        val saleDetails: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()

        partition.foreach {
          case (orderId, (orderInfoOption, orderDetailOption)) => {


            if (orderInfoOption != None) {

              val orderInfo: OrderInfo = orderInfoOption.get

              //①如果orderDetail Join上了，封装数据
              if (orderDetailOption != None) {

                val saleDetail = new SaleDetail(orderInfo, orderDetailOption.get)

                saleDetails.append(saleDetail)

              }

              //② orderInfo无条件写入redis


              // 一笔订单一个k-v对，设置过期时间，需要集群测试的最大网络延迟
              jedisClient.setex("order_info_" + orderId, 5 * 60, gson.toJson(orderInfo))


              //③从redis中尝试获取已经早到的order_detail，获取到就封装数据
              val earlyOrderDetails: util.Set[String] = jedisClient.smembers("order_detail_" + orderId)

              earlyOrderDetails.forEach(orderDetailStr => {

                val orderDetail: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])

                val saleDetail = new SaleDetail(orderInfo, orderDetail)

                saleDetails.append(saleDetail)

              })
            } else {

              // orderInfo为None， orderDetail一定不为None
              val orderDetail: OrderDetail = orderDetailOption.get

              // 先从redis中取出早到的orderInfo，进行关联
              val orderInfoStr: String = jedisClient.get("order_info_" + orderDetail.order_id)

              if (orderInfoStr != null) {

                val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])

                val saleDetail = new SaleDetail(orderInfo, orderDetail)

                saleDetails.append(saleDetail)

              } else {
                // 从redis没有取到相同订单的 order_info，说明 当前的order_detail 早来了，需要将order_detail写入redis，等待后续的order_info
                jedisClient.sadd("order_detail_" + orderDetail.order_id, gson.toJson(orderDetail))

                //设置过期时间
                jedisClient.expire("order_detail_" + orderDetail.order_id, 5 * 60)

              }
            }
          }
        }

        jedisClient.close()

        saleDetails.iterator

      })


      // 关联redis中的全量用户，为每笔订单详情的用户信息赋值
      val finalResult: DStream[SaleDetail] = joinedDatas.mapPartitions(partition => {

        val client: Jedis = RedisUtil.getJedisClient

        val saleDetails: Iterator[SaleDetail] = partition.map(saleDetail => {

          val userStr: String = client.get("user_" + saleDetail.user_id)

          val userInfo: UserInfo = JSON.parseObject(userStr, classOf[UserInfo])

          saleDetail.mergeUserInfo(userInfo)

          saleDetail

        })

        client.close()

        saleDetails

      })


      //转换为要写入到es的格式    id 取 orderDetailId
      val ds6: DStream[(String, SaleDetail)] = finalResult.map(saleDetail => (saleDetail.order_detail_id, saleDetail))

      //写入es
      ds6.foreachRDD(rdd => {

        //每天的明细都单独存放在一个索引中
        val indexName:String ="gmall2020_sale_detail"+ LocalDate.now()

        rdd.foreachPartition(partition => {

          val client: JestClient = MyEsUtil.getClient

          MyEsUtil.insertBulk(indexName,partition.toList)

          MyEsUtil.close(client)

        })

      })

    }

  }
}
