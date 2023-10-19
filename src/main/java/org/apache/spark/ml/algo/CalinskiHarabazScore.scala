package org.apache.spark.ml.algo

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{DenseMatrix => libDenseMatrix, Vectors => libVectors}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer


//在真实的分群label不知道的情况下，Calinski-Harabasz可以作为评估模型的一个指标。
// Calinski-Harabasz指标通过计算类中各点与类中心的距离平方和来度量类内的紧密度，
// 通过计算各类中心点与数据集中心点距离平方和来度量数据集的分离度，CH指标由分离度与紧密度的比值得到。
// 从而，CH越大代表着类自身越紧密，类与类之间越分散，即更优的聚类结果。

//s(k)=(tr(Bk)/tr(Wk))*((m−k)/(k−1)）
//其中m为训练样本数，k是类别个数，Bk是类别之间协方差矩阵，Wk是类别内部数据协方差矩阵，
// tr为矩阵的迹。也就是说，类别内部数据的协方差越小越好，类别之间的协方差越大越好，
// 这样的Calinski-Harabasz分数会高。同时，数值越小可以理解为：组间协方差很小，组与组之间界限不明显。

object CalinskiHarabazScore {
  def getCalinskHarabazScore(df:DataFrame,featureCol:String,labelCol:String) ={
    val totalmean = getCluserMeanValue(df,featureCol)
    var clusterTraceValue = 0.0
    var totalTraceValue = 0.0
    val labelArray = df.select(labelCol).distinct().rdd.map{case row:Row => row(0)}.collect().toArray
    for (label <- labelArray){
      val labelDf = df.filter(col(labelCol)===label)
      val labelClusterMatrix = getRowMatrix(labelDf,featureCol)
      val labelClusterMean = getCluserMeanValue(labelClusterMatrix,featureCol)
      totalTraceValue =  totalTraceValue + getDistance(labelClusterMean,totalmean)*labelDf.count()
      try
      {
        clusterTraceValue = clusterTraceValue + getClusterTraceValue(labelClusterMatrix,featureCol)*labelDf.count()
      }
      catch {
        case e:Throwable=>clusterTraceValue = clusterTraceValue + 0.0
      }
    }
    val Score = (totalTraceValue/clusterTraceValue)*((df.count()-labelArray.length)/(labelArray.length -1.0))
    Score
  }

  def getRowMatrix(dataset:DataFrame,featureCol:String)={
    val df = dataset.select(col(featureCol)).rdd.map {
      case Row(point: Vector) => libVectors.dense(point.toArray)
    }
    val mat: RowMatrix = new RowMatrix(df)
    mat
  }

  def getCovariance(dataset:RowMatrix,featureCol:String) ={
    val covariance = dataset.computeCovariance().asInstanceOf[libDenseMatrix]
    covariance
  }

  def getTraceValue(matr:libDenseMatrix) ={
    var index = 0
    val n = matr.numCols
    var traceTotal = 0.0
    while (index < n) {
      traceTotal = traceTotal + matr(index, index)
      index +=1
    }
    traceTotal
  }

  def getClusterTraceValue(dataset:DataFrame,featureCol:String) ={
    val rowM = getRowMatrix(dataset,featureCol)
    val totalCovariance = getCovariance(rowM,featureCol)
    getTraceValue(totalCovariance)
  }

  def getClusterTraceValue(dataset:RowMatrix,featureCol:String) ={
    val totalCovariance = getCovariance(dataset,featureCol)
    getTraceValue(totalCovariance)
  }

  def getCluserMeanValue(dataset:RowMatrix,featureCol:String) ={
    val colMean = dataset.computeColumnSummaryStatistics().mean
    colMean.toArray
  }

  def getCluserMeanValue(dataset:DataFrame,featureCol:String) ={
    val rowM = getRowMatrix(dataset,featureCol)
    val colMean = rowM.computeColumnSummaryStatistics().mean
    colMean.toArray
  }

  def getDistance(arrayA:Array[Double],arrayB:Array[Double]) ={
    val zipInfo = arrayA.zip(arrayB)
    var distance = 0.0
    zipInfo.foreach(line=>{
      distance = distance +math.pow(line._1-line._2,2)
    })
    //    math.sqrt(distance)
    distance
  }








  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[5]").appName("JoinAdResAndPcsearchbox").getOrCreate()

    val data = Array(
      libVectors.dense(4.0, 2.0, 3.0),
      libVectors.dense(5.0, 6.0, 1.0),
      libVectors.dense(2.0, 4.0, 7.0),
      libVectors.dense(3.0, 6.0, 5.0)
    )


    val rddData = spark.sparkContext.parallelize(data)


    // RDD转换成RowMatrix
    val mat: RowMatrix = new RowMatrix(rddData)
    val covariance = mat.computeCovariance().asInstanceOf[libDenseMatrix].asML

    println(covariance)
    var i = 0
    val n = covariance.numCols
    var total = 0.0
    while (i < n) {
      total = total + covariance(i, i)
       i +=1
      }
    println( total)
  }
}
