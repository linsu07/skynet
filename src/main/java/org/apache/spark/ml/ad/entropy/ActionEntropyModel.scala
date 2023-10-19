package org.apache.spark.ml.ad.entropy

import org.apache.spark.ml.common.{AntiSpamModel, CheckStatus, ModelPath, Utils}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, struct, udf,lit}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}

import scala.collection.mutable.ListBuffer

class ActionEntropyModel(override val uid:String)
  extends AntiSpamModel[ActionEntropyModel] with ActionEntropyParam with DefaultParamsWritable {
  //  var spamMap:Map[String,(Double,Int,Int)] = _
  var anomalyFrame:DataFrame = _
  var chlActionEntropy:Map[String,(Double,Int,Int,Array[(String,Int)],Array[Double])] = _
  var statDims:Array[String] = _
  var specDim:String = _
  //  def setSpamMap(map:Map[String,(Double,Int,Int)])={
  //    this.spamMap = map
  //    this
  //  }
  def setAnomalyFrame(f:DataFrame)={
    this.anomalyFrame = f
    this
  }
  def setEntropyMap(map:Map[String,(Double,Int,Int,Array[(String,Int)],Array[Double])])={
    this.chlActionEntropy = map
    this
  }

  //  var anomalyMap:Map[String,Set[String]] = _
  override def copy(extra: ParamMap): ActionEntropyModel = {
    copyValues(new ActionEntropyModel(uid).setParent(parent), extra)
  }
  //  override def write: MLWriter = new ActionEntropyModel.ModelWriter(this)

  def transfromStat(data: DataFrame): DataFrame = {
    val converted = chlActionEntropy.map{case (chl,(en,total,distinct,anomaly,featureEntropy))=>
      (chl,en,total,distinct,anomaly.mkString(","),featureEntropy.mkString(","))
    }.toSeq
    var ret = data
    for(i<-1 to(5)){
      val index = i
      val name = ${outputCols}(i)
      val add = udf{
        (row:Row,index:Int)=>
          val key = row.toSeq.mkString(Utils.sep)
          if(chlActionEntropy.contains(key)){
            val t = chlActionEntropy.get(key).get
            index match {
              case 1 => "%.3f".format(t._1)
              case 2=> t._2.toString
              case 3 => t._3.toString
              case 4=> t._4.map{case (key,value)=>s"${key}:${value}"}.mkString("|")
              case 5 => t._5.map("%.3f".format(_)).mkString("|")
            }
          }
          else
            ""
      }
      ret = ret.withColumn(name,add(struct(statDims.map(col):_*),lit(index)))
    }
    ret
  }

  def transformAnomalyScore(data: DataFrame): DataFrame = {
    val spamfeature = chlActionEntropy.map { case (chl, values) =>
      (chl, values._4.map(_._1).zip(values._5).toMap)
    }

    val addScore = udf{
      (row:Row)=>
        val values = row.toSeq
        val chl = row.toSeq.toArray.take(values.size-2).mkString(Utils.sep)
        val actionList = values(values.size-1).asInstanceOf[Seq[String]].mkString("->")
        if(spamfeature.get(chl).isDefined){
          if(spamfeature.get(chl).get.contains(actionList))
            spamfeature.get(chl).get.get(actionList).get
          else 0.0
        }else 0.0
    }
    val neededCols = new ListBuffer[String]()
    neededCols.appendAll(getStatDimension())
    neededCols.append(${inputCols}(0))
    data.asInstanceOf[DataFrame].withColumn(${outputCols}(0),addScore(struct(neededCols.map{col(_)}:_*)))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    statDims  = getStatDimension().take(getStatDimension().length-1)
    specDim = getStatDimension().last
    val data = dataset.asInstanceOf[DataFrame]
    if(data.schema.map(_.name).contains(specDim))
      transformAnomalyScore(data)
    //      dataset.asInstanceOf[DataFrame].join(anomalyFrame,getStatDimension().toSeq,"left_outer")
    //        .na.fill(0.0,Seq(${outputCols}(0)))
    else
      transfromStat(data)
  }
}

//object ActionEntropyModel extends MLReadable[ActionEntropyModel] with ModelPath {
//  class ModelWriter(instance: ActionEntropyModel) extends MLWriter {
//    override protected def saveImpl(path: String): Unit = {
//      // Save metadata and Params
//      DefaultParamsWriter.saveMetadata(instance, path, sc)
//      val entropyPath = getStatPath(path)
//      val converted = instance.chlActionEntropy.map{case (chl,(en,total,distinct,anomaly,featureEntropy))=>
//        (chl,en,total,distinct,anomaly.mkString(","),featureEntropy.mkString(","))
//      }.toSeq
//
//      val dataPath = getStatPath(path)
//      sparkSession.createDataFrame(converted)
//        .toDF("chl","entropy","total","distinct","anomaly_Counts","featureEntropy")
//        .repartition(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(dataPath)
//    }
//  }
//
//  override def read: MLReader[ActionEntropyModel] = new ActionEntropyModel.ModelReader()
//
//  private class ModelReader extends MLReader[ActionEntropyModel] {
//    /** Checked against metadata when loading model */
//    override def load(path: String): ActionEntropyModel = {
//      val model = new DefaultParamsReader[ActionEntropyModel].load(path)  // model 必须实现 this（uid:String）方法
//      model
//    }
//  }
//}