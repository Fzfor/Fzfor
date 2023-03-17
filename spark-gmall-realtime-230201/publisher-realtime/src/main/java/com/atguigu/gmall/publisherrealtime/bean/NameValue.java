package com.atguigu.gmall.publisherrealtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author fzfor
 * @date 11:15 2023/03/16
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@ToString
public class NameValue {
    private String name;
    private Object value;
}
