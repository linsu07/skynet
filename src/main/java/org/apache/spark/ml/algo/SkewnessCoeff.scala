package org.apache.spark.ml.algo

/**
  * @Program: anti-spam-skynet
  * @wikipedia https://en.wikipedia.org/wiki/Skewness
  * @Description 在概率论和统计学中，偏度衡量实数随机变量概率分布的不对称性。
  *              偏态系数以平均值与中位数之差对标准差之比率来衡量偏斜的程度，用SK表示偏斜系数:偏态系数小于0，
  *              因为平均数在众数之左，是一种左偏的分布，又称为负偏。偏态系数大于0，因为均值在众数之右，是一
  *              种右偏的分布，又称为正偏。
  * @Author: liuchengwei
  * @Email: liuchengwei1@360.cn
  * @Create: 2020-03-30 14:11
  **/


class SkewnessCoeff() {
  def getSkewnessCoeff(data: Array[Double]): Double = {
    val sum = data.sum
    val cnt = data.length
    val mean = sum / cnt
    val numerator = data.map(x => math.pow(x - mean, 3)).sum / cnt
    val denominator = math.pow((data.map(x => math.pow(x - mean, 2)).sum / cnt), 1.5)
    val skewness = numerator / denominator
    skewness
  }

  def getSkewnessCoeff(data: Array[String]): Double = {
    val dataDouble = data.map(_.toDouble)
    this.getSkewnessCoeff(dataDouble)
  }

  def getSkewnessCoeff(data: Array[Int]): Double = {
    val dataDouble = data.map(_.toDouble)
    this.getSkewnessCoeff(dataDouble)
  }

}


object SkewnessCoeffTest {
  def main(args: Array[String]): Unit = {
//    val info = Array[String]("1.0", "0.0", "0.2")
    var info = Array[Int](1, 2, 3, 4, 5, 6, 7, 8, 9)
    println(info.toSeq, new SkewnessCoeff().getSkewnessCoeff(info))
    info = Array[Int](0, 2, 3, 4, 5, 6, 7, 8, 9)
    println(info.toSeq, new SkewnessCoeff().getSkewnessCoeff(info))
    info = Array[Int](1, 2, 3, 4, 5, 6, 7, 8, 10)
    println(info.toSeq, new SkewnessCoeff().getSkewnessCoeff(info))

  }
}
