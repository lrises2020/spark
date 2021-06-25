package action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author Natasha
 * @Description 以数组的形式返回所有元素，所有的数据都会被拉取到Driver端(慎用)
 * @Date 2021/6/25 14:06
 **/
object collect {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //3.2 收集数据到Driver
    rdd.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
