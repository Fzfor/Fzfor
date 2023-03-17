package com.atguigu.springboot.springbootdemo.bean;

import lombok.*;

/**
 * @author fzfor
 * @date 15:01 2023/03/15
 */

//需要在idea中安装lombok插件

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class Customer {
    private String username;
    private String password;
    private String address;
    private Integer age;
    private String gender;

    //构造器（无参 + 有参）
    //get / set
    //tostring
    //......

    public static void main(String[] args) {
        Customer customer = new Customer();
        customer.getAddress();
    }
}
