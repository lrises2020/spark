import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author Natasha
 * @Description
 * @Date 2021/6/24 17:07
 **/
object 默认分区 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest1")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))

    rdd.saveAsTextFile("src/main/resources/默认分区")
  }
}
