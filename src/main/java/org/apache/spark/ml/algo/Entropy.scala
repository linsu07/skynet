package org.apache.spark.ml.algo

/**
  *@author linsu at 2019/11/04
  * 传入参数可以是未统计的类别型array，也可以是对每个类别统计好个数的整型array
  */

class InfoEntropy() extends Serializable{

  def getEntropy(attribution: Array[String]): Double = {
    val info = attribution.groupBy(identity).mapValues(_.length).values.toArray
    val sum = info.sum.toDouble
    val probList = info.map{_.toDouble/sum}
    innerGet(probList)
  }

  def getEntropy(info: Array[Int]): Double = {
    val sum = info.sum.toDouble
    val probList = info.map{_.toDouble/sum}
    innerGet(probList)
  }

  private def innerGet(l:Array[Double]): Double  ={
    l.map{ pro=>
      if(pro == 0) 0
      else -pro*math.log(pro)/math.log(2)
    }.sum
  }
  /*归一化的信息熵*/
  def getNormEntropy(info: Array[Int]):Double = {
    if (info.length <= 1)
      1.0
    else if (info.nonEmpty && info.sum != 0) {
      val length = info.length.toDouble
      val maxDisorder = info.map { _ => (1.0 / length) }
      getEntropy(info) / innerGet(maxDisorder)
    }
    else
      0.0
  }

}


object Entropy{
  def main(args: Array[String]): Unit = {
//    var info = Array[Int](1,0,0)
//    println(new InfoEntropy(info).getEntropy())
//    println(new InfoEntropy(info).getNormEntropy())
    val info = Array[Int]()
    println(new InfoEntropy().getEntropy(info))
//    println(new InfoEntropy(info).getNormEntropy())
  }
}