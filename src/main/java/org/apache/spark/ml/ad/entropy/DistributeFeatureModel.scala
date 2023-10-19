package org.apache.spark.ml.ad.entropy

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.common.{AntiSpamModel, Utils}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql

class DistributeFeatureModel  (override val uid:String, val stat:Array[RelativeEntropyDesc])extends AntiSpamModel[DistributeFeatureModel]{
  override def copy(extra: ParamMap): DistributeFeatureModel = ???

  var statDims:Array[String] = _
  var specDim:String = _

  def transformAnomalyScore(data: DataFrame): DataFrame = {
    val broadMap = data.sparkSession.sparkContext.broadcast(stat.map{desc:RelativeEntropyDesc=>
      (desc.key,desc.distribDiff)
    }.toMap)
    val calScore = udf{
      (row:Row,miniStatStr:String)=>
        val miniStat = miniStatStr.split("\\|").filter(_.length>0).map{ str=>
          val splits = str.split(":")
          (splits(0),splits(1).toDouble)}
        val total = miniStat.map(_._2).sum
        val groupKey = row.toSeq.mkString(Utils.sep)
        val diff = broadMap.value.getOrElse(groupKey,Map.empty[String,Double])
        if(miniStat.size>0)
          miniStat.map{ case(key,count)=>
            count.toDouble*diff.getOrElse(key,0.0)
          }.sum/total
        else
          0.0
    }

    if (getName().equals("adindustryDistributeBysite#srcg#guid")){
      val siteArray = Array("http://www.wb123.com","http://nav.bohaiqilin.com","https://www.52daohang.com","http://www.feihuo.com")
      val barMap = data.sparkSession.sparkContext.broadcast(stat.map{ desc:RelativeEntropyDesc=>
        (desc.key,desc.realDistribute)
      }.filter(k=>k._2.startsWith("游戏") && siteArray.contains(k._1.split(Utils.sep)(0))).toMap)
      val barInfo = barMap.value.keySet.toArray
      val barFun = udf{
        (row:Row)=>
          val groupKey = row.toSeq.mkString(Utils.sep)
          val site = row.getString(0)
          if (barMap.value.getOrElse(groupKey,"").nonEmpty)
            "1"
          else
            "0"
      }
      val metadata = new sql.types.MetadataBuilder().putStringArray("isBar", barInfo).build()
      data.withColumn(${outputCols}(0),calScore(struct(statDims.map(col):_*),col(${inputCols}(0))))
        .withColumn(${outputCols}(1),barFun(struct(statDims.map(col):_*)).as(${outputCols}(1),metadata))
    }
    else
      data.withColumn(${outputCols}(0),calScore(struct(statDims.map(col):_*),col(${inputCols}(0))))
  }

  def transfromStat(data: DataFrame): DataFrame = {
    val broadMap = data.sparkSession.sparkContext.broadcast(stat.map{desc:RelativeEntropyDesc=>
      (desc.key,Array(desc.Score.toString,desc.realDistribute
        ,desc.distribDiff.toArray.filter(_._2>0).sortBy(_._2).reverse.map{case (key,value)=>"%s:%.3f".format(key,value)}.mkString("|")
        ,desc.realRatioDistribute))
    }.toMap)
    val emptyStat = Array("0.0","","","")
    val addScoreCol = udf{
      (row:Row)=>
        val groupKey = row.toSeq.mkString(Utils.sep)
        broadMap.value.getOrElse(groupKey,emptyStat)(0)
    }
    val addDistributeCol = udf{
      (row:Row)=>
        val groupKey = row.toSeq.mkString(Utils.sep)
        broadMap.value.getOrElse(groupKey,emptyStat)(1)
    }
    val addDiffCol = udf{
      (row:Row)=>
        val groupKey = row.toSeq.mkString(Utils.sep)
        broadMap.value.getOrElse(groupKey,emptyStat)(2)
    }
    val addRatioDistributeCol = udf{
      (row:Row)=>
        val groupKey = row.toSeq.mkString(Utils.sep)
        broadMap.value.getOrElse(groupKey,emptyStat)(3)
    }

    if (getName().equals("adindustryDistributeBysite#srcg#guid")) {
      data.withColumn($ {outputCols}(2), addScoreCol(struct(statDims.map(col): _*)))
        .withColumn($ {outputCols}(3), addDistributeCol(struct(statDims.map(col): _*)))
        .withColumn($ {outputCols}(4), addDiffCol(struct(statDims.map(col): _*)))
        .withColumn($ {outputCols}(5), addRatioDistributeCol(struct(statDims.map(col): _*)))
    }
    else{
      data.withColumn($ {outputCols}(1), addScoreCol(struct(statDims.map(col): _*)))
        .withColumn($ {outputCols}(2), addDistributeCol(struct(statDims.map(col): _*)))
        .withColumn($ {outputCols}(3), addDiffCol(struct(statDims.map(col): _*)))
        .withColumn($ {outputCols}(4), addRatioDistributeCol(struct(statDims.map(col): _*)))
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val broadcastStandard = dataset.sparkSession.sparkContext.broadcast(stat)
    statDims  = getStatDimension().take(getStatDimension().length-1)
    specDim = getStatDimension().last
    val data = dataset.asInstanceOf[DataFrame]
    if(data.schema.map(_.name).contains(specDim))
      transformAnomalyScore(data)
    else
      transfromStat(data)
  }
}