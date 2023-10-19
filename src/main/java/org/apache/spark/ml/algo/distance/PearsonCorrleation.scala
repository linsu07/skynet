package org.apache.spark.ml.algo.distance

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.ml.linalg.{Matrices,Matrix}
/**
  * @Author zhubaowen
  * @create 2020/8/11 15:13
  */
trait PearsonCorrleation {

  def closeToZero(value: Double, threshold: Double = 1e-12): Boolean = {
    math.abs(value) <= threshold
  }

  def computeCorrelationMatrixFromCovariance(covarianceMatrix: Matrix): Matrix = {
    val cov = covarianceMatrix.asBreeze.asInstanceOf[BDM[Double]]
    val n = cov.cols
    // 计算对角元素的标准差
    var i = 0
    while (i < n) {
      cov(i, i) = if (closeToZero(cov(i, i))) 0.0 else math.sqrt(cov(i, i))
      i +=1
    }
    // Loop through columns since cov is column major
    var j = 0
    var sigma = 0.0
    var containNaN = false
    while (j < n) {
      sigma = cov(j, j)
      i = 0
      while (i < j) {
        val corr = if (sigma == 0.0 || cov(i, i) == 0.0) {
          containNaN = true
          Double.NaN
        } else {
          //根据上文的公式计算，即cov(x,y)/(sigma_x * sigma_y)
          cov(i, j) / (sigma * cov(i, i))
        }
        cov(i, j) = corr
        cov(j, i) = corr
        i += 1
      }
      j += 1
    }
    // put 1.0 on the diagonals
    i = 0
    while (i < n) {
      cov(i, i) = 1.0
      i +=1
    }
    Matrices.fromBreeze(cov)
  }
}
