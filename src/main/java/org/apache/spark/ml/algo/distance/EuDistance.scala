package org.apache.spark.ml.algo.distance

/**
  * @author linsu 2020/08/05
  */

import org.apache.spark.ml.Estimator
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.ml.common.AntiSpamAlgorithm
import breeze.linalg.{inv, DenseMatrix => BDM}
import org.apache.spark
import org.apache.spark.ml.linalg.{DenseMatrix, Matrices, Matrix, Vectors}
import org.apache.spark.mllib.linalg.{DenseMatrix => libDenseMatrix, Vectors => libVectors}
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.ml.param.{DoubleArrayParam, DoubleParam, IntParam, Param, ParamMap, Params, StringArrayParam}
import org.apache.spark.mllib.linalg
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
trait  EuDistanceParam extends Params{
  val meanVector = new DoubleArrayParam(this,"vector","分布的时候用到")
  val meanArray = new DoubleArrayParam(this,"meanArray","通常有一个值")
  val sigmaArray = new DoubleArrayParam(this,"sigmaArray","通常有一个值")
  val cov= new DoubleArrayParam(this,"corV","协方差的value")
  val covSize = new IntParam(this,"corvSize","corv的feature个数")
  val outputPath = new Param[String](this,"outputPath","算法输出路径")
  val spamThreshold = new DoubleParam(this,"spamThreshold","读取配置文件中的作弊阈值")
  val sigmaOffset = new IntParam(this,"sigmaOffset","sigma偏移量")
  setDefault(
    spamThreshold->15.0
    ,sigmaOffset->3
  )
  def getSpamThreshold()={
    ${spamThreshold}
  }
  def setSpamThreshold(threshold:Double)={
    set(spamThreshold->threshold)

  }

  def getSigmaOffset()={
    ${sigmaOffset}
  }
  def setSigmaOffset(sigmaValue:Int)={
    set(sigmaOffset->sigmaValue)
  }

  def getOutputPath(path: String)={
    ${outputPath}
  }

  def setConv(matrix:DenseMatrix)={
    set(covSize->matrix.numCols)
    set(cov->matrix.values)
    this
  }

  def getConv()={
    new DenseMatrix(${covSize},${covSize},${cov})
  }
  def getMeanVector = {
    Vectors.dense(${meanVector})
  }
  def getMean = {
    ${meanArray}(0)
  }
  def getSigma = {
    ${sigmaArray}(0)
  }
  def getMeanArray = {
    ${meanArray}
  }
  def getSigmaArray ={
    ${sigmaArray}
  }
  def setMeanVector(arr:Array[Double])={
    set(meanVector->arr)
  }
  def setMean(arr:Array[Double]) = {
    set(meanArray->arr)
  }
  def setSigma(arr:Array[Double]) = {
    set(sigmaArray->arr)
  }

  def setOutputPath(str:String) = {
    set(outputPath->str)
  }
}
class EuDistance(override val uid: String)extends Estimator[EuDistanceModel] with AntiSpamAlgorithm with EuDistanceParam with PearsonCorrleation {
  set(name->"EuDistance")
  def this() = this("EuDistance")
  override def fit(dataset: Dataset[_]): EuDistanceModel = {
    val dataframe = dataset.asInstanceOf[DataFrame]
    val rdd = dataframe.select(${inputCols}.toSeq.map(col):_*).rdd.map{ row=>
      val features = row.toSeq.map(_.toString.toDouble).toArray
      libVectors.dense(features)
    }
    val rowMatrix = new RowMatrix(rdd)
    //对矩阵按列统计
    val stasticSummary: MultivariateStatisticalSummary = rowMatrix.computeColumnSummaryStatistics()
//    //非零元素个数
//    val numnonezeros = stasticSummary.numNonzeros.toArray
//    //列元素个数
//    val count = stasticSummary.count
//    //非零占比
//    val ratioExpr = ${inputCols}
//      .zip(numnonezeros)
//      .map(k=>k._1 + ":" + "%.3f".format(k._2 / count))
//      .mkString(",")
//    println(s"${this.uid} standard numnonezeros ratio is  = ${ratioExpr}")
    //均值
    val ma: Array[Double] = stasticSummary.mean.toArray
    val meanExpr = ${inputCols}.zip(ma).map(k=>k._1 + ":" + "%.3f".format(k._2)).mkString(",")
    println(s"${this.uid} standard mean is  = ${meanExpr}")
    //标准差
    val va = stasticSummary.variance.toArray.map(math.sqrt(_))
    val varExpr = ${inputCols}.zip(va).map(k=>k._1 + ":" + "%.3f".format(k._2)).mkString(",")
    println(s"${this.uid} standard vari is  = ${varExpr}")
    //协方差矩阵
    val covariance = rowMatrix.computeCovariance().asInstanceOf[libDenseMatrix].asML
    println("in EuDistance, Covariance is")
    println(covariance.toString(10,160))
    //协方差矩阵的逆
    val inverseCoV = Matrices.fromBreeze(inv(covariance.asBreeze.asInstanceOf[BDM[Double]])).asInstanceOf[DenseMatrix]
    println("EuDistance inverted covariance is ")
    println(inverseCoV.toString(20,160))
//    //皮尔森系数
//    println("in EuDistance, pearson correlation is")
//    val covariance1 = covariance.copy
//    val pearson = computeCorrelationMatrixFromCovariance(covariance1)
//    println(pearson.toString(20,160))


    setMean(ma).setSigma(va)
    val model = copyValues(new EuDistanceModel(${name}))
    model.write.overwrite().save(${outputPath})
    model
  }


  override def copy(extra: ParamMap): Estimator[EuDistanceModel] = {this}
  override def transformSchema(schema: StructType): StructType = {schema}
}
