import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author Natasha
 * @Description
 * @Date 2021/6/24 17:15
 **/
object 自定义分区 {
  def main1(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest1")
    val sc: SparkContext = new SparkContext(conf)

    //1）默认分区的数量：默认取值为当前核数和2的最小值
    //val rdd: RDD[String] = sc.textFile("input")

    //2）输入数据1-4，每行一个数字；输出：0=>{1、2} 1=>{3} 2=>{4} 3=>{空}
    //val rdd: RDD[String] = sc.textFile("input/3.txt",3)

    //3）输入数据1-4，一共一行；输出：0=>{1234} 1=>{空} 2=>{空} 3=>{空}
    val rdd: RDD[String] = sc.textFile("input/4.txt", 3)

    rdd.saveAsTextFile("output")

    sc.stop()
  }

  def main2(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest1")
    val sc: SparkContext = new SparkContext(conf)

    //1）4个数据，设置4个分区，输出：0分区->1，1分区->2，2分区->3，3分区->4
    //val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 4)

    //2）4个数据，设置3个分区，输出：0分区->1，1分区->2，2分区->3,4
    //val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 3)

    //3）5个数据，设置3个分区，输出：0分区->1，1分区->2、3，2分区->4、5
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5), 3)

    rdd.saveAsTextFile("src/main/resources/从集合中自定义分区")

    sc.stop()
  }
}
