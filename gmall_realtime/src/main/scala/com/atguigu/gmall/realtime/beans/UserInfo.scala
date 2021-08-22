package com.atguigu.gmall.realtime.beans

/**
 * Created by VULCAN on 2021/4/29
 */
case class UserInfo(id:String,
                    login_name:String,
                    user_level:String,
                    birthday:String,
                    gender:String)


case class OrderDetail(id:String,
                       order_id: String,
                       sku_name: String,
                       sku_id: String,
                       order_price: String,
                       img_url: String,
                       sku_num: String)
