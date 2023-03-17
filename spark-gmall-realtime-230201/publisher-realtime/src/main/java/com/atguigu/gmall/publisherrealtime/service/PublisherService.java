package com.atguigu.gmall.publisherrealtime.service;

import com.atguigu.gmall.publisherrealtime.bean.NameValue;

import java.util.List;
import java.util.Map;

/**
 * @author fzfor
 * @date 18:29 2023/03/15
 */
public interface PublisherService {
    Map<String, Object> doDauRealtime(String td);

    List<NameValue> doStateByItem(String itemName, String date, String t);

    Map<String, Object> doDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize);

}
