package com.etiantian

import org.junit.Test
import org.apache.flink.api.java.tuple._

import scala.reflect.ClassTag

class GenericTest {

  @Test
  def testGetGenericType() = {
    genericType[Tuple3[String, Int, Double]](new Tuple3[String, Int, Double]("few", 2323, 324.2342))
  }

  def genericType[T: ClassTag](a: T) = {

    val runtimeClassInfo = implicitly[ClassTag[T]].runtimeClass
    //    val b = a.newInstance()
    val a = "3333333"
    val instance = runtimeClassInfo.newInstance().asInstanceOf[T]
    val methods = runtimeClassInfo.getDeclaredMethods.filter(m => m.getName.equals("setField"))
    val method = methods(0)
    println(instance)
    println(method)
    println(method.getParameterTypes.length)
    method.invoke(instance, new java.lang.String("234"), new Integer(2))
    println(instance)
    //    for (i <- 0 to 2)
    //      method(0).invoke(instance, "333",i)
  }


  def getType[T: ClassTag]() ={

  }
}
