package com.atguigu.canal.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.gmall.constansts.GmallConstants;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

/**
 * Created by VULCAN on 2021/4/26
 *
 *      步骤：  ①创建一个客户端
 *              ②使用客户端连接上canal server
 *              ③订阅canal server中指定库，指定表的binlog日志
 *              ④向canal server拉取数据
 *              ⑤解析拉取到的数据
 *              ⑥将解析后的数据写入到kafka
 *
 *
 *       核心：了解canalclient 返回的数据的结构和格式！
 *
 *              Message：  拉取的一批数据，一批可能是多个sql造成的写操作变化
 *                       List<Entry> entries = new ArrayList();
 *
 *              Entry：  一个sql写操作变化
 *                      关键属性：  tablename： 表名,通过Entry.Header获取
 *                                  EntryType: 语句类型。
 *                                                  只有ROWDATA类型的语句，例如insert,update,delete会影响数据的变化
 *                                                  例如create语句,alter语句会引起表结构的变化
 *                                  StoreValue:  写操作变化
 *                                                      只有ROWDATA会记录StoreValue，序列化数据，需要先反序列化再使用
 *
 *                                                反序列化： 使用工具类RowChange，会将StoreValue反序列化为RowChange对象！
 *
 *                                   RowChange： 发生的所有行的变化！
 *                                         关键属性：  EventType: 语句类型
 *                                                      RowDataList:  所有变化的行
 *                                                             RowData:   变化的一行
 *                                                                  Column: 变化的一行中的一列
 *
 *
 *
 *
 */
public class MyCanalClient {

    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {

        /*
                SimpleCanalConnector： 独立模式的canal连接器
                ClusterCanalConnector： 集群模式的canal连接器，多了故障转移功能。
                                            如果连接的主canal server挂掉，自动切换到当前新的canal server挂掉

                canal集群模式： 可以在集群上部署多个canal server，构成主从关系。
                                一旦主canal server挂掉 ，从的canal server可以切换为主机，避免单点故障！


                   SocketAddress address： canal server的主机名和端口号
                   String destination:  canal server订阅的mysql的实例的instance.properties文件所在的目录名
                    String username: 客户端连接canal server需要提供的用户名
                    String password: 客户端连接canal server需要提供的密码

         */
        //创建一个客户端
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop103", 11111),
                "example", null, null);


       // ②使用客户端连接上canal server
        canalConnector.connect();

        /*
                ③订阅canal server中指定库，指定表的binlog日志

                如果subscribe()，参考服务端的fitler,在instance.properties中有canal.instance.filter.regex和canal.instance.filter.black.regex
                决定了，当前的canal server只会订阅 目标mysql实例的哪些表。

                canal.instance.filter.regex：白名单，只订阅白名单上的，默认 .*\\..*，代表所有表
                canal.instance.filter.black.regex： 黑名单 ，除了黑名单的都订阅


                subscribe(filter)： 以自己编写的filter为主，支持正则表达式


         */
        canalConnector.subscribe("gmall1130.*");

        /*
            源源不断地，反复拉取数据

            Message[id=-1,entries=[],raw=false,rawEntries=[]]: 如果当前没有拉取到数据，Messaage中的id为-1
         */
        while(true){

            Message message = canalConnector.get(100);

            //当前没有拉取到数据
            if (message.getId() == -1){

                //歇一会，再去拉取
                System.out.println("当前没有最新数据，歇会再去拉取!");

                Thread.sleep(5000);

                continue;

            }

            //解析拉取到的数据
            //System.out.println(message);

            //获取所有拉取到的sql语句引起的变化
            List<CanalEntry.Entry> entries = message.getEntries();

            for (CanalEntry.Entry entry : entries) {

                if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){

                    //获取到表名
                    String tableName = entry.getHeader().getTableName();

                    //获取到StoreValue
                    ByteString storeValue = entry.getStoreValue();

                    parseData(tableName,storeValue);


                }
            }

        }

    }

    // 解析EntryType.ROWDATA的数据
    private static void parseData(String tableName, ByteString storeValue) throws InvalidProtocolBufferException {


        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

        /*
                GMV需求，获取order_info表，新增订单的total_amount字段

                    order_info：  可能发生的变化
                                        insert ： 新增订单
                                        update :  订单的状态更新

         */
        //将order_info的insert的数据写入到kafka
        if (tableName.equals("order_info") && rowChange.getEventType().equals(CanalEntry.EventType.INSERT)){

            sendDataToKafka(GmallConstants.GMALL_ORDER_INFO,rowChange);

            //将order_detail的insert的数据写入到kafka
        }else  if (tableName.equals("order_detail") && rowChange.getEventType().equals(CanalEntry.EventType.INSERT)){

            sendDataToKafka(GmallConstants.GMALL_ORDER_DETAIL,rowChange);

        }else  if (tableName.equals("user_info") &&
                (rowChange.getEventType().equals(CanalEntry.EventType.INSERT) || rowChange.getEventType().equals(CanalEntry.EventType.UPDATE))){

            sendDataToKafka(GmallConstants.GMALL_USER_INFO,rowChange);

        }

    }

    // 将RowChange中的数据，写入到kafka的某个主题
    public  static  void  sendDataToKafka(String topic, CanalEntry.RowChange rowChange){
        //获取所有变化的行
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

        for (CanalEntry.RowData rowData : rowDatasList) {

            //获取每一行变化的列名和变化后的列值
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            //先将每一行的所有的列值和列名，构建为一个JsonStr
            JSONObject jsonObject = new JSONObject();

            for (CanalEntry.Column column : afterColumnsList) {

                jsonObject.put(column.getName(),column.getValue());

            }

            /*int delayTimes = new Random().nextInt(10);

            try {
                //模拟网络延迟
                Thread.sleep(delayTimes * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            MyKafkaProducer.sendMessageToKafka(topic,jsonObject.toJSONString());

        }
    }
}
