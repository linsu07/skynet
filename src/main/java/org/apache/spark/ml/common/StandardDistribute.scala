package org.apache.spark.ml.common

import org.apache.spark.ml.Model
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{DenseMatrix, Vector, Vectors}
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols}
import org.apache.spark.ml.param.{DoubleArrayParam, IntParam, Param, ParamMap, Params}
import org.apache.spark.ml.search.clickbehavior.{ClickGapModel, ClickGapParam}
import org.apache.spark.ml.stat.Summarizer.{mean, variance}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, MLWritable}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer

trait StandardDistribute extends Params{
  val meanVector = new DoubleArrayParam(this,"vector","分布的时候用到")
  val meanArray = new DoubleArrayParam(this,"valueArray","通常有一个值")
  val sigmaArray = new DoubleArrayParam(this,"sigmaArray","通常有一个值")
  val cov= new DoubleArrayParam(this,"corV","协方差的value")
  val covSize = new IntParam(this,"corvSize","corv的feature个数")
  val scoreName = new Param[String](this,"scoreName","for output score")

  def setConv(matrix:DenseMatrix)={
      set(covSize->matrix.numCols)
      set(cov->matrix.values)
      this
  }

  def getScoreName()={${scoreName}}

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
}

trait Norm {
  def normalize(stat:Dataset[_]):StandardModel

}
class StandardModel (override val uid: String)
  extends Model[StandardModel] with DefaultParamsWritable with StandardDistribute with HasInputCol{
  var frame:DataFrame = _
  set(scoreName->s"${uid}_score")
  def this()=this("standard")
  def statistic(frame:DataFrame)={
    this.frame = frame
    import frame.sparkSession.implicits._
    val (m,v) = frame.select(mean(col(StandardModel.featuresCol),col(StandardModel.weightCol))
      ,variance(col(StandardModel.featuresCol),col(StandardModel.weightCol))).as[(Vector,Vector)].first()
    val ma = m.toArray
    setMean(ma)
    val va = v.toArray.map(math.sqrt(_))
    setSigma(va)
    println(s"${this.uid} standard mean is  prop = ${ma.map("%.3f".format(_)).mkString(",")}")
    println(s"${this.uid} standard vari is  prop = ${va.map("%.3f".format(_)).mkString(",")}")
  }
  def freeFrame()={
    frame.unpersist(true)
  }
  override def copy(extra: ParamMap): StandardModel = ???

  override def transform(dataset: Dataset[_]): DataFrame = {
//    def getShort(orginal:Array[Double]): Array[Double] ={
//      val buf = new ListBuffer[Double]()
//      buf.append(orginal(0))
//      buf.append(orginal(1))
//      buf.append(orginal(4))
//      buf.toArray
//    }
//    if(${meanArray}.length==5){
//      set(meanArray->getShort(${meanArray}))
//      set(sigmaArray->getShort(${sigmaArray}))
//    }
    println(s"transform  standard ${uid} ...")
    val calScore = udf {
      (v: Vector) =>
        val curValues = v.toArray
      val ret = new ListBuffer[Double]()

      for(i<-0 until(curValues.length)){
      val normalized =math.min((curValues(i)-${meanArray}(i))/${sigmaArray}(0),3.0)
      // 以右侧3sigma为原点， 最大不超过 9个sigma 单位做为value
      val score =math.min( (3.0 -normalized),9.0)
      ret.append(score)
      }
      Vectors.dense(ret.toArray)
    }
    dataset.asInstanceOf[DataFrame].withColumn(getScoreName(),calScore(col(uid)))
  }

  override def transformSchema(schema: StructType): StructType = schema

}
object StandardModel extends DefaultParamsReadable[StandardModel]{
  var keyCol:String = "key"
  var featuresCol:String = "features"
  var weightCol:String = "weight"
}
class StandardSumModel (override val uid: String)
  extends Model[StandardSumModel]  with StandardDistribute with HasInputCols{
  def this() = this("StandardAvg")
  set(scoreName->s"${uid}_score")
  override def copy(extra: ParamMap): StandardSumModel = ???
  def setInputs(colNames:Array[String]): Unit ={
    set(inputCols->colNames)
  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    val vectorAssembler = new VectorAssembler()
    vectorAssembler.setInputCols(${inputCols})
    vectorAssembler.setOutputCol("StandardSumAssembler")
    var ret = vectorAssembler.transform(dataset)
    val calDistance = udf{
      (features:Vector)=>
        Vectors.dense(Array(features.toArray.sum/features.toArray.length.toDouble))
    }
    ret.withColumn(getScoreName(),calDistance(col("StandardSumAssembler")))
  }

  override def transformSchema(schema: StructType): StructType = ???
}