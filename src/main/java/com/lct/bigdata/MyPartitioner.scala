package com.lct.bigdata

import org.apache.spark.Partitioner

import scala.collection.mutable

class MyPartitioner(ins: Array[String]) extends Partitioner{
  private val partMap = new mutable.HashMap[String, Int]()
  var num=0;
  for (elem <- ins) {
    partMap.put(elem,num)
    num+=1
  }
//  设置分区的个数
  override def numPartitions: Int = partMap.size
//根据key获取该条数据放到那个分区里面
  override def getPartition(key: Any): Int = {
//    根据key获取对应的分区号，如果没有可以对应的值，就返回0，放到0号分区
    partMap.getOrElse(key.toString,0)
  }
}
