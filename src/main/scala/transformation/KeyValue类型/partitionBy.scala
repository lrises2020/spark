package transformation.KeyValue类型

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author Natasha
 * @Description 按照key指定partition重新进行分区
 * @Date 2021/6/25 13:47
 **/
object partitionBy {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)

    //3.2 对RDD重新分区
    val rdd2: RDD[(Int, String)] = rdd.partitionBy(new org.apache.spark.HashPartitioner(3))


    val indexRdd: RDD[(Int, String)] = rdd2.mapPartitionsWithIndex(
      (index, datas) => {
        // 打印每个分区数据，并带分区号
        datas.foreach(data => {
          println(index+ "=>" + data)
        })
        // 返回分区的数据
        datas
      }
    )

    indexRdd.collect()

    //4.关闭连接
    sc.stop()
  }
}
