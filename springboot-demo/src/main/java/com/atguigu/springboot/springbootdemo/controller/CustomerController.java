package com.atguigu.springboot.springbootdemo.controller;

import com.atguigu.springboot.springbootdemo.bean.Customer;
import com.atguigu.springboot.springbootdemo.service.CustomerService;
import com.atguigu.springboot.springbootdemo.service.impl.CustomerServiceImpl;
import com.atguigu.springboot.springbootdemo.service.impl.CustomerServiceImplNew;
import org.apache.catalina.filters.AddDefaultCharsetFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.lang.model.element.VariableElement;

class MyFactoryUtils {
    public static CustomerService newInstance() {
        //return  new CustomerServiceImpl();
        return new CustomerServiceImplNew();
    }
}

/**
 * @author fzfor
 * @date 12:53 2023/03/11
 */
//@Controller //标识为控制层（spring）
@RestController  //@Controller + @ResponseBody
public class CustomerController {

    //直接在类中创建对象，不好，因为写死了，如果后续更换service，需要改controller的代码
    //CustomerServiceImpl customerService = new CustomerServiceImpl();


    //接口:
    //CustomerService customerService = new CustomerServiceImpl(); //等号右边本质没变

    //接口 + 工厂：
    //CustomerService customerService = MyFactoryUtils.newInstance();

    //SpringBoot
    @Autowired //从spring容器中找到对应类型的对象，注入过来
    @Qualifier("customerServiceImplNew") //明确指定将哪个对象注入过来
    CustomerService customerService;

    /**
     * http://localhost:8080/login?username=zhangsan&age=22
     */
    @GetMapping("login")
    public String login(@RequestParam("username") String username,
                        @RequestParam("password") String password) {
        //业务处理
        //在每个方法中创建业务层对象，不好
        //CustomerServiceImpl service = new CustomerServiceImpl();
        String result = customerService.doLogin(username, password);
        return result;
    }


    /**
     * 常见的状态码
     * 200：表示请求处理成功且相应成功
     * 302：表示进行重定向
     * 400：表示请求参数有误
     * 404：表示请求地址或者资源不存在
     * 405：表示请求方式不支持
     * 500：表示服务器端处理异常
     *
     * http://localhost:8080/statuscode
     */
    @GetMapping("statuscode")
    public String statusCode(@RequestParam("username") String username, @RequestParam("age") Integer age) {
        //String str = null;
        //str.length();

        return username + ", " + age;
    }

    /**
     * 请求方式：GET POST PUT DELETE　......
     * GET: 读
     *
     * POST: 写
     *
     * http://localhost:8080/requestmethod
     */
    //@RequestMapping(value = "requestmethod", method = RequestMethod.GET)
    //@GetMapping("requestmethod")
    @PostMapping("requestmethod")
    public String requestMethod(){
        return "success";
    }

    /**
     * 请求参数：
     *      1.地址栏中的kv格式的参数
     *      2.嵌入到地址栏中的参数
     *      3.封装到请求体中的参数
     */

    /**
     * 3.封装到请求体中的参数
     * http://localhost:8080/parambody
     *
     * 请求体中的参数：
     *      username=weiyunhui
     *      password=123123
     *
     * 如果请求参数名与方法的形参名不一致，需要通过@RequestParam来标识获取
     * 如果一致，可以直接映射
     *
     *
     * @RequestBody: 将请求体中的json格式的参数映射到对象对应的属性上。
     */

    @RequestMapping("parambody")
    public Customer parambody(@RequestBody Customer customer){
        return customer; //转换成json字符串返回
    }
    /*
    @RequestMapping("parambody")
    public String parambody(String username, String password){
        return "username = " + username + ", password = " + password;
    }
    */

    /**
     * 2.嵌入到地址栏中的参数
     * http://localhost:8080/parampath/lisi/22?address=beijing
     * @PathVariable:将请求路径中的参数映射到请求方法对应的形参上
     */
    @RequestMapping("parampath/{username}/{age}")
    public String parampath(@PathVariable("username") String username,
                            @PathVariable("age") Integer age,
                            @RequestParam("address") String address) {
        return "username = " + username + ", age = " + age + ", address = " + address;
    }



    /**
     * 1.地址栏中的kv格式的参数
     * http://localhost:8080/paramkv?username=zhangsan&age=22
     *
     * @RequestParam: 将请求参数映射到方法对应的形参上
     *                如果请求参数名和方法形参名一致，可以直接进行参数值的映射，可以省略@RequsetParam
     */
    @RequestMapping("paramkv")
    public String paramkv(@RequestParam("username") String user, @RequestParam("age") Integer age) {
        return "username = " + user + ", age = " + age;
    }





    /**
     * 客户端请求；http://localhost:8080/helloworld
     * 请求处理方法：
     *
     * @RequestMapping:将客户端的请求与方法进行映射
     *
     * @ResponseBody : 将方法的返回值处理成字符串（json）返回给客户端
     */
    @RequestMapping("helloworld")
    //@ResponseBody
    public String helloworld() {

        return "success";
    }

    /**
     * 客户端请求；http://localhost:8080/hello
     * 请求处理方法：
     *
     * @RequestMapping:将客户端的请求与方法进行映射
     *
     * @ResponseBody : 将方法的返回值处理成字符串（json）返回给客户端
     */
    @RequestMapping("hello")
    //@ResponseBody
    public String hello() {

        return "hello java";
    }

    @RequestMapping("hello.hh")
    //@ResponseBody
    public String helloHh() {

        return "hello.hh";
    }
}
