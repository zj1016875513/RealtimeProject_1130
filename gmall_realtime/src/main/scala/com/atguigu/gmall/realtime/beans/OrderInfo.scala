package com.atguigu.gmall.realtime.beans

/**
 * Created by VULCAN on 2021/4/26
 */
case class OrderInfo(
                      id: String,
                      province_id: String,
                      consignee: String,
                      order_comment: String,
                      var consignee_tel: String,
                      order_status: String,
                      payment_way: String,
                      user_id: String,
                      img_url: String,
                      total_amount: Double,
                      expire_time: String,
                      delivery_address: String,
                      create_time: String,
                      operate_time: String,
                      tracking_no: String,
                      parent_order_id: String,
                      out_trade_no: String,
                      trade_body: String,
                    // 创建日期
                      var create_date: String,
                    // 创建的小时
                      var create_hour: String)
