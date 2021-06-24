import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author Natasha
 * @Description
 * @Date 2021/6/24 16:45
 **/
object 从集合中创建RDD {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.使用parallelize()创建rdd
    val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8))

    rdd.foreach(println)

    println("分区数：" + rdd.partitions.size)


    /*
    //4.使用makeRDD()创建rdd
    val rdd1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8))

    rdd1.foreach(println)
    println("分区数：" + rdd1.partitions.size)
    */

    sc.stop()
  }
}
