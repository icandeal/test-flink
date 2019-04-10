package com.etiantian

import org.junit.Test
import org.apache.flink.api.java.tuple._
import org.json.JSONObject

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

  @Test
  def testCopyList(): Unit = {
    var a = List[(String, String, Int, Int)]()
    val newList = List(
      ("http://url1", "aaa", 3, 26),
      ("http://url2", "aaa", 3, 29),
      ("http://res", "aaa", 3, 31),
      ("http://url3", "aaa", 3, 44),
      ("http://res", "aaa", 3, 56),
      ("http://url5", "aaa", 3, 76),
      ("http://url6", "aaa", 3, 81),
      ("http://url7", "aaa", 3, 99)
    )

    var list = List[(String, String, String, Int, Int, Int)]()

    for (i <- 0 until newList.length) {
      if (newList(i)._1.equals("http://res")) {
        a = newList.slice(i, i + 400)
        println("=================================================")
        a.foreach(println)
        val url1: String = if (a.length>=2) a(1)._1.toString else null
        val url2: String = if (a.length>=3) a(2)._1.toString else null
        val url3: String = if (a.length>=4) a(3)._1.toString else null
        val cost1 = if (a.length>=2) a(1)._4 else 0
        val cost2 =if (a.length>=3) a(2)._4 else 0
        val cost3 =if (a.length>=4) a(3)._4 else 0

          list = (url1, url2, url3, cost1, cost2, cost3) +: list

        println("=================================================\n\n")
      }
    }
    list = list.reverse

    println(list)
  }

  @Test
  def testJson(): Unit = {
    val json = new JSONObject("{\"partition\":0,\"offset\":24,\"topic\":\"ubuntu.school.user_info\",\"value\":\"{\\\"before\\\":null,\\\"after\\\":{\\\"ref\\\":25,\\\"username\\\":\\\"eee\\\",\\\"password\\\":null,\\\"comment\\\":null},\\\"source\\\":{\\\"version\\\":\\\"0.9.2.Final\\\",\\\"connector\\\":\\\"mysql\\\",\\\"name\\\":\\\"ubuntu\\\",\\\"server_id\\\":1,\\\"ts_sec\\\":1554883564,\\\"gtid\\\":null,\\\"file\\\":\\\"mysql-bin.000003\\\",\\\"pos\\\":5288,\\\"row\\\":0,\\\"snapshot\\\":false,\\\"thread\\\":0,\\\"db\\\":\\\"school\\\",\\\"table\\\":\\\"user_info\\\",\\\"query\\\":null},\\\"op\\\":\\\"c\\\",\\\"ts_ms\\\":1554883564157}\",\"key\":\"{\\\"ref\\\":25}\"}")
    println(new JSONObject(json.get("value").toString))
  }
}
