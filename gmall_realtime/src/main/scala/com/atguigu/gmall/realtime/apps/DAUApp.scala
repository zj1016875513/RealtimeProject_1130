package com.atguigu.gmall.realtime.apps

import java.{lang, util}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constansts.GmallConstants
import com.atguigu.gmall.realtime.beans.StartUpLog
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

/**
 * Created by VULCAN on 2021/4/23
 *
 *     只将从kafka中消费的每个 设备当天最早启动的那条启动日志写入到hbase!
 *
 *        从 1天中，每个设备的N条启动日志中，过滤(去重)取出最早那条。
 *
 *        状态：         上一批计算的结果
 *        有状态的计算：  当前这批数据在计算时，需要用到上一批计算的结果
 *                         upStateByKey
 *                         需要设置checkpoint目录
 *                           StreamingContext.checkpoint(HDFS路径)
 *
 *                           弊端： 周期性产生小文件
 *                           不用sparkstreaming提供的有状态的计算的算子，选择自己维护状态!
 *
 *
 *                           步骤： ①每批次计算完成后，自己把需要保存的数据，写入到 高性能的数据库中
 *                                  ②每批次计算前，手动从数据库读取上一次保存的状态
 *
 *
 *        无状态的计算：  当前这批数据在计算时，无需用到上一批计算的结果
 *
 *
 *        ①从kafka中消费数据，封装为样例类
 *        ②跨批次，进行状态的计算(跨批次去重，跨批次去重，读取redis中已经保存的状态，判断是否已经写过了)
 *            将状态保存到redis，如何设计的key,value的类型
 *              存的是什么？    存的是某一天，已经向hbase中存储过的启动日志的设备的 mid
 *                K-V :
 *                      key:   DAU:当天的日期
 *                      value: set
 *                              string
 *                              hash
 *
 *                              list,set,zset
 *
 *        ③同批次的去重，同批次中如果出现同一个设备启动了多次，需要选出启动时间最早那条
 *              同一批次有：  2021-04-23-mid1
 *                            2021-04-23-mid1
 *                            2021-04-24-mid1
 *
 *            去重后：2021-04-23-mid1，2021-04-24-mid1
 *
 *            去重算子：  distinct,groupby,groupByKey
 *
 *
 *
 *        ④将去重后的设备id，写入到redis(保存状态，给下一批次使用)
 *        ⑤将去重后的完整的 启动日志，写入hbase
 *
 */
object DAUApp extends  BaseApp {

  override val appName: String = "DAUApp"
  override val internal: Int = 10



  def main(args: Array[String]): Unit = {

    //先封装StreamingContext
    streamingContext=new StreamingContext("local[*]",appName,Seconds(internal))

    runApp{

      val ds: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_STARTUP, streamingContext)

      //  ①从kafka中消费数据，封装为样例类
      val ds2: DStream[StartUpLog] = ds.map(record => {

        val dateTimeFormatter1: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val dateTimeFormatter2: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")

        val jsonStr: String = record.value()

        val startUpLog: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])

        //继续为其封装logDate，logHour属性
        val localDateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(startUpLog.ts), ZoneId.of("Asia/Shanghai"))

        startUpLog.logDate = localDateTime.format(dateTimeFormatter1)
        startUpLog.logHour = localDateTime.format(dateTimeFormatter2)

        startUpLog
      })

      ds2.count().print()

      //②跨批次，进行状态的计算(跨批次去重，跨批次去重，读取redis中已经保存的状态，判断是否已经写过了)
      //val ds3: DStream[StartUpLog] = removeDuplicateMids3(ds2, streamingContext)

      val ds3: DStream[StartUpLog] = removeDuplicateMids2(ds2)

      ds3.count().print()

      //③同批次的去重，同批次中如果出现同一个设备启动了多次，需要选出启动时间最早那条
      val ds4: DStream[StartUpLog] = removeDuplicateMidsWithCommonBatch(ds3)

      ds4.count().print()

      // ④将去重后的设备id，写入到redis(保存状态，给下一批次使用)
      ds4.foreachRDD( rdd => {

        rdd.foreachPartition(partition => {

          val jedisClient: Jedis = RedisUtil.getJedisClient

          //写入redis
          partition.foreach(startUpLog => {

            jedisClient.sadd("DAU:" + startUpLog.logDate , startUpLog.mid)

          })

          //设置1天后过期
          jedisClient.expire("DAU:"+LocalDate.now() , 24 * 60 * 60)

          jedisClient.close()

        })

      })


      //⑤将去重后的完整的 启动日志，写入hbase
      ds4.foreachRDD(rdd => {
        rdd.saveToPhoenix("GMALL2020_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          //不能new Configuration ,只能使用HBaseConfiguration.create()
          // new Configuration，只会读取hadoop相关的配置文件中的参数， HBaseConfiguration.create() 不仅读取hadoop相关的配置文件，还会读取
          //hbase相关的配置我呢就
          HBaseConfiguration.create(),
          Some("hadoop102:2181")

        )
      })

    }

  }


  /*
        使用groupByKey进行去重 ,只有RDD[K,V]才能调用
              Key:    日期 + 设备号
              Value:  当前的startUpLog

               同一批次有：  2021-04-23-mid1
 *                            2021-04-23-mid1
 *                            2021-04-24-mid1
 *
 *            去重后：2021-04-23-mid1，2021-04-24-mid1
   */
  def removeDuplicateMidsWithCommonBatch(ds3: DStream[StartUpLog]) :DStream[StartUpLog]= {

    val result: DStream[StartUpLog] = ds3.transform(rdd => {

      val rdd1: RDD[((String, String), StartUpLog)] = rdd.map(startUpLog => ((startUpLog.logDate, startUpLog.mid), startUpLog))

      // 从Iterable[StartUpLog] 挑选出 ts最小的 记录
      val rdd2: RDD[((String, String), Iterable[StartUpLog])] = rdd1.groupByKey()

      // ((String, String), Iterable[StartUpLog])  ====>  ((String, String), StartUpLog)   =====>  StartUpLog
      val resultRDD: RDD[StartUpLog] = rdd2.flatMap {
        case ((logDate, mid), startUpLogs) => {

          val minTsStartUpLog: List[StartUpLog] = startUpLogs.toList.sortBy(_.ts).take(1)
          minTsStartUpLog

        }
      }
      resultRDD

    })
    result

  }


  /*
      效率低：
          这批数据有2000条
              从池中借2000次连接，发2000次请求，redis服务端处理2000次请求，归还2000次连接
   */
  def removeDuplicateMids1(ds2: DStream[StartUpLog]):DStream[StartUpLog] = {

    //判断当前这批数据中，哪些已经在redis中存在了，如果存在过滤掉，不存在的留下来   filter
    val result: DStream[StartUpLog] = ds2.filter(startUpLog => {

      //连接redis，判断
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //判断一个元素是否在set集合中存在  sismember
      val isExists: lang.Boolean = jedisClient.sismember("DAU:" + startUpLog.logDate, startUpLog.mid)

      //归还连接
      jedisClient.close()

      !isExists

    })

    result

  }


  /*

            这批数据有2000条,假设分两个区，以分区为单位从池中借连接
                从池中借2次连接，发2000次请求，redis服务端处理2000次请求，归还2次连接

                判断member是否在set集合中的运算，由redis服务器完成！


        以分区为单位操作的算子：
              mapPartitions:  有返回值，需要返回值，用这个
              foreachPartition : 返回值为Unit，无返回值，无需返回值，用这个


              无需考虑redis的负载！
     */
  def removeDuplicateMids2(ds2: DStream[StartUpLog]):DStream[StartUpLog] ={

    val result: DStream[StartUpLog] = ds2.mapPartitions(partition => {

      val jedisClient: Jedis = RedisUtil.getJedisClient

      println(Thread.currentThread().getName + ":" + jedisClient)
      // 处理这个分区的数据
      val filterdLogs: Iterator[StartUpLog] = partition.filter(startUpLog => {

        val isExists: lang.Boolean = jedisClient.sismember("DAU:" + startUpLog.logDate, startUpLog.mid)
        !isExists
      })

      jedisClient.close()

      filterdLogs

    })
    result

  }


  /*

           这批数据有2000条,假设分两个区，以分区为单位从池中借连接
               从池中借1次连接，发1次请求，redis服务端处理1次请求，归还1次连接

                  判断member是否在set集合中的运算，不能由redis服务器完成，因为只能发送1次请求！
                  判断member是否在set集合中的运算，由spark完成！
                  让spark下载redis中存储的set集合！可以使用广播变量提供效率！


       以分区为单位操作的算子：
             mapPartitions:  有返回值，需要返回值，用这个
             foreachPartition : 返回值为Unit，无返回值，无需返回值，用这个


        在哪个位置广播？

          在sparkstreaming的程序中，使用广播变量，判断广播变量和每批计算的数据之间的关系！
              如果要广播的是永恒不变的变量，可以在Driver端只广播一次！
              如果广播的变量，每次会随着每批数据的计算发生变化，又需要每次都用到变化后最新的数据，广播变量，需要每一批运算开始前，都广播一次！


        如何实现，每一批运算前广播？
            写在DStream的算子api中！
              RDD中定义的方法叫算子！  丰富！
              DStream中定义的方法叫抽象原语！  匮乏！

              如果想执行的算子，在RDD中有，但是DStream中没有提供，可以使用transform，允许调用RDD的算子来处理DStream
                transform： def transform[U: ClassTag](transformFunc: RDD[T] => RDD[U]): DStream[U]


         弊端： 广播的变量最好不要超过300M
                无需担心。spark会采取LRU的回收策略，将最近最少使用的广播变量，清理！

    */
  def removeDuplicateMids3(ds2: DStream[StartUpLog],context: StreamingContext):DStream[StartUpLog] ={

    val result: DStream[StartUpLog] = ds2.transform(rdd => {

      val jedisClient: Jedis = RedisUtil.getJedisClient

      //从redis端下载set集合
      val mids: util.Set[String] = jedisClient.smembers("DAU:" + LocalDate.now())

      jedisClient.close()

      //此位置广播   不可取   只会在Driver端，广播一次
      val bc: Broadcast[util.Set[String]] = context.sparkContext.broadcast(mids)
      //对rdd进行分区处理，在分区处理之前，希望广播变量

      val resultRDD: RDD[StartUpLog] = rdd.filter(startUpLog => {

        !bc.value.contains(startUpLog.mid)
      })

      resultRDD

    })

    result
  }


  /*
      如果遇到跨天的数据如何不误判

          redis中存储时，可以让每天的每个设备生成一条key-value对

              key：  DAU:+日期+mid
              value:  string

              redis存在两个key:   DAU:2021-04-23mid1
                                  DAU:2021-04-23mid2


   */
  def removeDuplicateMids4(ds2: DStream[StartUpLog]):DStream[StartUpLog]={

    val result: DStream[StartUpLog] = ds2.mapPartitions(partition => {

      val jedisClient: Jedis = RedisUtil.getJedisClient

      // 处理这个分区的数据
      val filterdLogs: Iterator[StartUpLog] = partition.filter(startUpLog => {

        val isExists: lang.Boolean = jedisClient.exists("DAU:" + startUpLog.logDate + startUpLog.mid)

        !isExists
      })

      jedisClient.close()

      filterdLogs

    })
    result

  }

}
