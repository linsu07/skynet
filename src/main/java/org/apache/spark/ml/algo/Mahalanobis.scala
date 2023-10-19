package org.apache.spark.ml.algo

/**
  * @author linsu 2020/03/05
  */
import breeze.linalg.{inv, DenseMatrix => BDM}
import org.apache.spark.ml.common.{StandardDistribute, StandardModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{DenseMatrix, Matrices, Vector, Vectors}
import org.apache.spark.ml.param.{ParamMap, Params, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{DenseMatrix => libDenseMatrix, Vectors => libVectors}
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

class Mahalanobis(val bestPoint:Array[Double],val inversed:DenseMatrix) extends Serializable {
  var metric:Double = _
  def setMetric(metric: Double): Mahalanobis = {
    this.metric = metric
    this
  }

  def distance(curPoint:Vector)={
    val relocatedPoint = curPoint.toArray.zip(bestPoint).map{case(cur,best)=>cur-best}
//    val relocatedPoint = curPoint.toArray
    val x=new DenseMatrix(1,relocatedPoint.length,relocatedPoint)
    val xTranspose = x.transpose
    var ret = x.multiply(inversed)
    ret = ret.multiply(xTranspose)
    ret.values(0)
//    Vectors.dense(Array(math.sqrt(score)))
  }
  def score(curPoint:Vector) ={
    val score = distance(curPoint)/metric
    Vectors.dense(Array(math.sqrt(score)))
  }
}
object Mahalanobis{
  def apply(mean:Array[Double],variance:Array[Double],covariance:DenseMatrix): Mahalanobis = {
    println("MahaDistance covariance is ")
    println(covariance.toString(20,160))
    println(s"mean is ${mean.mkString(",")}")
    println(s"variance is ${variance.mkString(",")}")
    val bestPoint = {
      mean.zip(variance).map{case (m,v)=>
        m+3.0*v
      }
    }
//    val bestPoint = mean
    println(s"bestPoint is ${bestPoint.mkString(",")}")
    val inverseCoV = Matrices.fromBreeze(inv(covariance.asBreeze.asInstanceOf[BDM[Double]])).asInstanceOf[DenseMatrix]
    println("MahaDistance inverted covariance is ")
    println(inverseCoV.toString(20,160))

    val maha = new Mahalanobis(bestPoint,inverseCoV)
    val sigma2 = {
      mean.zip(variance).map{case (m,v)=>
        m+2.0*v
      }
    }
    val metric = maha.distance(Vectors.dense(sigma2))
    maha.setMetric(metric)
  }
}
trait MahaDistanceParam extends Params{
  val inputCols = new StringArrayParam(this,"inputCols","输入feature所在的列")
  def setInputs(inputs:Array[String]): Unit ={
    println("MahaDistance inputs is %s".format(inputs.mkString(",")))
    set(inputCols->inputs)
  }
}
class MahaDistance(override val uid:String) extends Estimator[MahaDistanceModel] with MahaDistanceParam{
  def this() = this(MahaDistanceModel.uid)
  override def fit(dataset: Dataset[_]): MahaDistanceModel = {
    val positions = dataset.schema.map(_.name).zipWithIndex.filter{case(name,_)=>
      name.equals(StandardModel.featuresCol)
      }.map(_._2).toArray

    val ret = dataset.asInstanceOf[DataFrame].rdd.map{ row=>
      val allVecotor = positions.map{row.getAs[Vector](_).toArray}
      val ret = new ListBuffer[Double]()
      allVecotor.foreach(v=>ret.appendAll(v))
      libVectors.dense(ret.toArray)
    }.cache()
    var rowMatrix = new RowMatrix(ret)
    var stasticSummary: MultivariateStatisticalSummary =rowMatrix.computeColumnSummaryStatistics()
    var ma = stasticSummary.mean.toArray
    var meanExpr = ma.map("%.3f".format(_)).mkString(",")
    println(s"${this.uid} standard mean is  = ${meanExpr}")
    var va = stasticSummary.variance.toArray.map(math.sqrt(_))
    var varExpr = va.map("%.3f".format(_)).mkString(",")
    println(s"${this.uid} standard vari is  = ${varExpr}")

//    val newRet = ret.map{v=>
//      val normalized = v.toArray.zipWithIndex.map{case (v,i)=>
//        (v-ma(i))/va(i)
//      }
//      libVectors.dense(normalized)
//    }
//    rowMatrix = new RowMatrix(newRet)
    val Covariance = rowMatrix.computeCovariance().asInstanceOf[libDenseMatrix].asML
    println("in MahaDistance, Covariance is")
    println(Covariance.toString(10,160))
//    stasticSummary =rowMatrix.computeColumnSummaryStatistics()
//    ma = stasticSummary.mean.toArray
//    meanExpr = ma.map("%.3f".format(_)).mkString(",")
//    println(s"${this.uid} standard mean is  = ${meanExpr}")
//    va = stasticSummary.variance.toArray.map(math.sqrt(_))
//    varExpr = va.map("%.3f".format(_)).mkString(",")
//    println(s"${this.uid} standard vari is  = ${varExpr}")
    val model = copyValues(new MahaDistanceModel(uid))
    .setMean(ma)
    .setSigma(va)
    .setConv(Covariance).asInstanceOf[MahaDistanceModel]
    model
  }
  override def copy(extra: ParamMap): Estimator[MahaDistanceModel] = {this}
  override def transformSchema(schema: StructType): StructType = {schema}
}
class MahaDistanceModel(override val uid:String) extends  Model[MahaDistanceModel] with StandardDistribute
  with MahaDistanceParam with DefaultParamsWritable{
  set(scoreName->s"${uid}_score")
  override def copy(extra: ParamMap): MahaDistanceModel = {this}
  def getMahalanobis={
     Mahalanobis(${meanArray},${sigmaArray},getConv())
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    println(s"transform ${uid} ... ")
//    def getShort(orginal:Array[Double]): Array[Double] ={
//      val buf = new ListBuffer[Double]()
//      buf.append(orginal(0))
//      buf.append(orginal(1))
//      buf.append(orginal(4))
//      buf.append(orginal(6))
//      buf.toArray
//    }
//    if(${meanArray}.length==7){
//      set(meanArray->getShort(${meanArray}))
//      set(sigmaArray->getShort(${sigmaArray}))
//      val buf = new ListBuffer[Double]()
//    }
    val maha = getMahalanobis
    val vectorAssembler = new VectorAssembler()
    vectorAssembler.setInputCols(${inputCols})
    vectorAssembler.setOutputCol("vectorAssembler")
    var ret = vectorAssembler.transform(dataset)
    val calDistance = udf{
      (features:Vector)=>
        maha.score(features)
    }
    ret.withColumn(getScoreName(),calDistance(col("vectorAssembler")))
  }
  override def transformSchema(schema: StructType): StructType = {schema}
}

object MahaDistanceModel extends DefaultParamsReadable[MahaDistanceModel]{
  val uid = "MahaDistance"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val path = "/tmp/wap/standard/"
    val model = MahaDistanceModel.load(path+uid)
    val maha = model.getMahalanobis
    val sigma3 = {
      model.getMeanArray.zip(model.getSigmaArray).map{case (m,v)=>
        m+2.0*v
      }
    }

    var distance = maha.distance(Vectors.dense(sigma3))
    println("zero distance is %.4f".format(distance))

    val sigma2 = {
      model.getMeanArray.zip(model.getSigmaArray).map{case (m,v)=>
        m+1.0*v
      }
    }
    distance = maha.distance(Vectors.dense(sigma2))
    println("zero distance is %.4f".format(distance))

    val sigma1 = {
      model.getMeanArray.zip(model.getSigmaArray).map{case (m,v)=>
        m+0.0*v
      }
    }
    distance = maha.distance(Vectors.dense(sigma1))
    println("zero distance is %.4f".format(distance))
    spark.close()
  }
}

