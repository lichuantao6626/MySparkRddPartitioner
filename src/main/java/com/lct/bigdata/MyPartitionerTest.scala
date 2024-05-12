package com.lct.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
通过自定义partitioner，控制相同key的数据在同一个分区，在输出的时候，输出到同一个文件。
 */
object MyPartitionerTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("test01")
      .getOrCreate()
    val key_arr = Array[String]("a","b","c")
    val rdd01: RDD[String] = spark.sparkContext.textFile("file:///E:\\临时文件\\2024-05-12\\test01.txt")
    val rdd02: RDD[(String, Int)] = rdd01.flatMap(line => line.split(" ")).map(word => (word, 1))
    val arrs: Array[(String, Int)] = rdd02.reduceByKey((a, b) => a + b).collect()
//    for (ele <- arrs) {
//      println(ele)
//    }

    val rdd04: RDD[(String, Int)] = rdd02.partitionBy(new MyPartitioner(key_arr))
    rdd04.saveAsTextFile("file:///E:\\临时文件\\2024-05-12\\out")
    spark.stop()

  }

}
