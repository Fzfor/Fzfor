package com.atguigu.gmall.publisherrealtime.mapper;

import com.atguigu.gmall.publisherrealtime.bean.NameValue;

import java.util.List;
import java.util.Map;

/**
 * @author fzfor
 * @date 18:31 2023/03/15
 */
public interface PublisherMapper {
    Map<String, Object> searchDau(String td);

    List<NameValue> searchStatsByItem(String itemName, String date, String field);

    Map<String, Object> searchDetailByItem(String date, String itemName, int from, Integer pageSize);
}
