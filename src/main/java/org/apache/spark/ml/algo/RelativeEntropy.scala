package org.apache.spark.ml.algo

import org.apache.spark.ml.linalg.Vector

import scala.collection.mutable.ListBuffer
/**
  * @author linsu
  * @param p 标准或者理想的分布
  * @param q 实际当前的分布
  */
class RelativeEntropy(p:Array[Double],q:Array[Double]) {
  def this(p:Vector,q:Vector)={
    this(p.toArray,q.toArray)
  }
  val a = new ListBuffer[Double]()
  def apply(p: Array[Double], q: Array[Double]): RelativeEntropy = new RelativeEntropy(p, q)
  def score():Double={
    val sumP = p.sum
    val sumQ = q.sum
    val proportionP = p.map(_/sumP).map{ v=>
      if(v==0) RelativeEntropy.epsilon
      else v
    }
    val proportionQ = q.map(_/sumQ)
    proportionP.zip(proportionQ).map{case (pValue,qValue)=>
      pValue*math.log(pValue/(qValue+RelativeEntropy.epsilon))
    }.sum
  }

  def distributionJudge(DiffThreshold:Double=0.0):Array[(Int,Double)]={
    val judge_info = new ListBuffer[(Int,Double)]
    val sumP = p.sum
    val sumQ = q.sum
    val proportionP = p.map(_/sumP)
    val proportionQ = q.map(_/sumQ)
    for(i <- proportionP.indices){
      val diff = proportionQ(i) -  proportionP(i)
      if (diff >DiffThreshold)
        judge_info.append((i,diff))
    }
    judge_info.toArray
  }
}
object RelativeEntropy{
  val epsilon = math.pow(10,-7)
}





