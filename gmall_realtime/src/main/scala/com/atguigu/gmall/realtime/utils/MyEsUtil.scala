package com.atguigu.gmall.realtime.utils

import java.util
import java.util.Objects

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}
import collection.JavaConverters._

object MyEsUtil {

  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

  /**
   * 获取客户端
   *
   * @return jestclient
   */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
   * 关闭客户端
   */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
   * 建立连接
   */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)
  }

  /*
      批量插入数据到ES

      必须将数据封装为  (K,V),放入一个List集合，K必须是id,V是插入的数据
   */
  def insertBulk(indexName: String, docList: List[(String, Any)]): Unit = {

    if (docList.size > 0) {
      val jest: JestClient = getClient
      // 创建index时，必须为index设置type为_doc
      val bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
      for ((id, doc) <- docList) {
        val indexBuilder = new Index.Builder(doc)
        if (id != null) {
          indexBuilder.id(id)
        }
        val index: Index = indexBuilder.build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()

      var items: util.List[BulkResult#BulkResultItem] = null
      try {
        // items 代表写成功了多少条数据
        items = jest.execute(bulk).getItems
      } catch {
        case ex: Exception => println(ex.toString)
      } finally {
        close(jest)
        println("保存" + items.size() + "条数据")
        /*
              <- 用于遍历一个scala的集合   将java集合转为scala集合
                ①需要导入转换的隐式方法  import collection.JavaConverters._
                ②scala转java   scala集合.asJava
                ③java转scala   java集合.asScala
         */
        for (item <- items.asScala) {
          if (item.error != null && item.error.nonEmpty) {
            println(item.error)
            println(item.errorReason)
          }
        }
      }
    }
  }
}


