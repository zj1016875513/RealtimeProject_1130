package com.atguigu.gmall.realtime.apps

import org.apache.spark.streaming.StreamingContext

/**
 *
 *    控制抽象
 *       scala没有break关键字，提供一个方法实现break关键字的效果
 *
     * def breakable(op: => Unit) {
     *   try {
     *     op
     *   } catch {
     *  case ex: BreakControl =>
     *  if (ex ne breakException) throw ex
     * }
     * }
 *
 *      op:  一段返回值为unit的代码
 *
 *
 *
 *
 * Created by VULCAN on 2021/4/23
 *    BaseApp 封装了4个 不同的需求，从kafka中消费数据，启动sparkstreaming应用的 通用套路！
 *        ①构建 StreamingContext
 *        ②使用StreamingContext 获取一个DStream
 *        ③使用DStream 进行各种转换
 *        ④启动应用
 *        ⑤阻塞应用，让其一致运行
 *
 *      每个需求，允许传入自定义的逻辑！
 *          ③
 *
 *
 *
 *     在使用时，子类继承BaseApp，重写抽象属性appName，internal，streamingContext，调用runApp{ 自己的转换逻辑 }
 */
abstract class BaseApp {

  // 应用程序的名称
  val appName:String

  //spark streamingapp的采集周期
  val internal:Int

  var streamingContext:StreamingContext=null

  // 封装sparkstreaming app的运行的通用流程
  def runApp(op: => Unit): Unit ={

    try {
      // 每个需求自己的转换逻辑
      op
    } catch {
      case  e:Exception=> println(e.getMessage)
    }

    streamingContext.start()

    streamingContext.awaitTermination()


  }


}
