package com.atguigu.gmall.realtime.util

import com.atguigu.gmall.realtime.bean.{DauInfo, PageLog}

import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks

/**
 * 实现对象属性拷贝
 *
 * @author fzfor
 * @date 15:02 2023/02/20
 */
object MyBeanUtils {

  def main(args: Array[String]): Unit = {
    val pageLog: PageLog = PageLog("mid1001", "u1001", "p1123", null, null, null, null, null, null, null, null, null, null, 0L, null, 123321L)
    val dauInfo = new DauInfo()
    println("copy前：" + dauInfo)
    copyProperties(pageLog, dauInfo)
    println("copy后：" + dauInfo)
  }

  /**
   * 将srcObj中属性的值拷贝到dest对应的属性上
   *
   * @param srcObj
   * @param destObj
   */
  def copyProperties(srcObj: AnyRef, destObj: AnyRef): Unit = {
    if (srcObj == null || destObj == null) {
      return
    }

    //获取到srcObj中所有的属性
    val srcFields: Array[Field] = srcObj.getClass.getDeclaredFields

    //处理每个属性的拷贝
    for (srcField <- srcFields) {

      Breaks.breakable {
        //get / set
        //sacla会自动为类中的属性提供get set方法
        //get: fieldname()
        //set: fieldName_$eq(参数类型)

        //getMethodName
        val getMethodName: String = srcField.getName
        val setMethodName: String = srcField.getName + "_$eq"

        //从srcObj中获取get方法对象
        val getMethod: Method = srcObj.getClass.getDeclaredMethod(getMethodName)
        //从destObj中获取set方法对象
        val setMethod: Method = {
          try {
            destObj.getClass.getDeclaredMethod(setMethodName, srcField.getType)
          } catch {
            case exception: Exception => Breaks.break()
          }
        }
        //忽略 val 属性
        val destField: Field = destObj.getClass.getDeclaredField(srcField.getName)
        if (destField.getModifiers.equals(Modifier.FINAL)) {
          Breaks.break()
        }

        //调用get方法获取到srcObj属性的值，再调用set方法将获取到的属性值赋值给destObj的属性
        setMethod.invoke(destObj, getMethod.invoke(srcObj))
      }
    }
  }
}
