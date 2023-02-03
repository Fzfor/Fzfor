package com.atguigu.gmall.realtime.bean

/**
 * @author fzfor
 * @date 11:15 2023/02/03
 */
case class PageDisplayLog(mid :String,
                          user_id:String,
                          province_id:String,
                          channel:String,
                          is_new:String,
                          model:String,
                          operate_system:String,
                          version_code:String,

                          brand: String,

                          page_id:String ,
                          last_page_id:String,
                          page_item:String,
                          page_item_type:String,
                          sourceType: String,
                          during_time:Long,
                          display_type:String,
                          display_item: String,
                          display_item_type:String,
                          display_order:String ,
                          display_pos_id:String,
                          ts:Long)

