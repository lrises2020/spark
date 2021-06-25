package transformation.KeyValue类型

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @Author Natasha
 * @Description
 * @Date 2021/6/25 13:51
 **/
object 自定义分区器partitionBy {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)
    //3.2 自定义分区
    val rdd3: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))

    //4 打印查看对应分区数据
    val indexRdd: RDD[(Int, String)] = rdd3.mapPartitionsWithIndex(
      (index, datas) => {
        // 打印每个分区数据，并带分区号
        datas.foreach(data => {
          println(index + "=>" + data)
        })
        // 返回分区的数据
        datas
      }
    )

    indexRdd.collect()

    //5.关闭连接
    sc.stop()
  }
}

// 自定义分区
class MyPartitioner(num: Int) extends Partitioner {
  // 设置的分区数
  override def numPartitions: Int = num

  // 具体分区逻辑
  override def getPartition(key: Any): Int = {

    if (key.isInstanceOf[Int]) {

      val keyInt: Int = key.asInstanceOf[Int]
      if (keyInt % 2 == 0)
        0
      else
        1
    } else {
      0
    }
  }
}