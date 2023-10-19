package org.apache.spark.ml.common

import org.apache.spark.sql.Row

import scala.collection.mutable

/**
  * @author linsu at 2019/11/29
  *  statMap 中，第一个key固定是算法的名字，第二个key是统计口径的名字
  */
trait AggregateByKey {
  def seqOp(statMap:mutable.HashMap[String,mutable.HashMap[String,mutable.HashMap[String,Double]]], row:Row):mutable.HashMap[String,mutable.HashMap[String,mutable.HashMap[String,Double]]]
  def combOp(map1:mutable.HashMap[String,mutable.HashMap[String,mutable.HashMap[String,Double]]]
             ,map2:mutable.HashMap[String,mutable.HashMap[String,mutable.HashMap[String,Double]]])
  :mutable.HashMap[String,mutable.HashMap[String,mutable.HashMap[String,Double]]]
}
