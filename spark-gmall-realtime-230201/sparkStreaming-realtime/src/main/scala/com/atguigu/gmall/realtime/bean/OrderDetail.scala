package com.atguigu.gmall.realtime.bean

/**
 * @author fzfor
 * @date 21:36 2023/02/21
 */
case class OrderDetail(
                        id: Long,
                        order_id: Long,
                        sku_id: Long,
                        order_price: Double,
                        sku_num: Long,
                        sku_name: String,
                        create_time: String,
                        split_total_amount: Double = 0D,
                        split_activity_amount: Double = 0D,
                        split_coupon_amount: Double = 0D
                      ) {

}
