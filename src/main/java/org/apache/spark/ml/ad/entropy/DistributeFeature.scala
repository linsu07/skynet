package org.apache.spark.ml.ad.entropy

/**
  *@author linsu at 2020/07/20 10:45  宁静的夏天
  */
import com.typesafe.config.Config
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.algo.RelativeEntropy
import org.apache.spark.ml.common.{AntiSpamAlgorithm, Utils}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.{DoubleArrayParam, DoubleParam, IntParam, Param, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

trait DistributeFeatureParam extends Params {
  val standardDistribute = new Param[Map[String,ListBuffer[Double]]](this,"standardDistribute","标准分布每个项对应的比例/均值/标准差")
  val standardDistributeSecond = new Param[Map[String,ListBuffer[Double]]](this,"standardDistributeSecond","标准分布每个项对应的比例/均值/标准差")
  val remitKey = new Param[Map[String,ListBuffer[Double]]](this,"remitKey","豁免的key的标准差")
  val dividend = new Param[String](this,"dividend","指定统计分母")
}
case class RelativeEntropyDesc(key:String,Score:String,realDistribute:String,distribDiff:Map[String,Double],realRatioDistribute:String)
class DistributeFeature(override val uid:String) extends Estimator[DistributeFeatureModel]
  with AntiSpamAlgorithm with DistributeFeatureParam{
  def this() = this(Identifiable.randomUID("DistributeFeature"))
  override def setConfig(c: Config): DistributeFeature.this.type = {
    super.setConfig(c)
    val standard = c.getConfig("standardDistribute")
    set(standardDistribute->standard.root().keySet().asScala.map{key=>
      var buf = new ListBuffer[Double]
      standard.getDoubleList(key).asScala.map(k=>buf += k)
      (key,buf)
    }.toMap)
    if (c.hasPath("standardDistributeSecond")) {
      val standard = c.getConfig("standardDistributeSecond")
      set(standardDistributeSecond->standard.root().keySet().asScala.map { key =>
        var buf = new ListBuffer[Double]
        standard.getDoubleList(key).asScala.map(k => buf += k)
        (key, buf)
      }.toMap)
    }
    else
      set(standardDistributeSecond->Map())

    if(c.hasPath("remitKey")){
      val remitTemp = c.getConfig("remitKey")
      set(remitKey->remitTemp.root().keySet().asScala.map{key=>
        var buf = new ListBuffer[Double]
        remitTemp.getDoubleList(key).asScala.map(k => buf += k)
        (key, buf)}
        .toMap)
    }
    else
      set(remitKey->Map())

    if(c.hasPath("dividend"))
      set(dividend->c.getString("dividend"))  //value为"first"代表第一个key的value做分母
    else
      set(dividend->"total")

    this
  }

  def getDifference(standard:Map[String,ListBuffer[Double]],realDistribute:Array[(String,Double)],groupKey:String, barInfo:Set[String]=Set()): Map[String,Double] ={
    val real = realDistribute.sortBy(_._1)
    standard.toArray.sortBy(_._1).zip(real).map{ case ((key,standRatio),(key2,realRatio))=>
      var resRatioStd = (realRatio-standRatio.head)/(standRatio.last + RelativeEntropy.epsilon)
      if(${remitKey}.nonEmpty && barInfo.contains(groupKey) && ${remitKey}.contains(key.toString))
      {
        resRatioStd = (realRatio-${remitKey}(key).head)/(${remitKey}(key).last + RelativeEntropy.epsilon)
      }
      (key,{if (resRatioStd>0) resRatioStd else 0.0})
    }.toMap
  }

  //没有做分母条件的修改
  def getDifference2(standard:Map[String,Double],realDistribute:mutable.HashMap[String,Double]): Map[String,Double] ={
    val ret = new mutable.HashMap[String,Double]()
    var maxAnomaly = 0.0
    do {
      var countSum:Double =  realDistribute.values.sum + RelativeEntropy.epsilon
      val real = realDistribute.map{case(key,value)=>(key,value/countSum)}.toArray.sortBy(_._1)
      val maxEnrrupt = standard.toArray.sortBy(_._1).zip(real).map { case ((key, standRatio), (key2, realRatio)) =>
        (key, (realRatio - standRatio))
      }.maxBy(_._2)
      maxAnomaly = maxEnrrupt._2.toString.toDouble
      val maxKey = maxEnrrupt._1
      if(ret.contains(maxKey)==false&&maxAnomaly>0.02){
        ret.put(maxKey,maxAnomaly)
        val leftCount = realDistribute.filter(_._1.equals(maxKey)==false).values.sum
        val assumeCount = (leftCount/standard.filter(_._1.equals(maxKey)==false).values.sum)*standard.get(maxKey).get
        realDistribute.put(maxKey,assumeCount)
      }
      else
        maxAnomaly = 0.0
    }while(maxAnomaly>0.02)
    ret.toMap
  }

  override def fit(dataset: Dataset[_]): DistributeFeatureModel = {
    //    val broadcastStandard = dataset.sparkSession.sparkContext.broadcast(${standardDistribute})
    val statDims  = getStatDimension().take(getStatDimension().length-1)
    val specDim = getStatDimension().tail
    val data = dataset.asInstanceOf[DataFrame]
    var barInfo = Set[String]()
    if(${inputCols}.length>1 && data.columns.contains(${inputCols}(1)))
      barInfo = data.schema(${inputCols}(1)).metadata.getStringArray(${inputCols}(1)).toSet
    val statInfo=data.rdd.aggregate(new mutable.HashMap[String,mutable.HashMap[String,Double]])(
      (map:mutable.HashMap[String,mutable.HashMap[String,Double]],row)=>{
        val groupKey = AntiSpamAlgorithm.getStatKey(statDims,row)
        val keyCountMap = map.getOrElseUpdate(groupKey,new mutable.HashMap[String,Double]())
        val miniStatStr = row.getAs[String](${inputCols}(0))
        miniStatStr.split("\\|").filter{str=>str.length>=3&&str.contains(":")}.map{ str=>
          val splits = str.split(":")
          (splits(0),splits(1))
        }.foreach{case (key,value)=> keyCountMap.put(key,value.toDouble+keyCountMap.getOrElse(key,0.0))}
        map
      },
      (map1:mutable.HashMap[String,mutable.HashMap[String,Double]],map2:mutable.HashMap[String,mutable.HashMap[String,Double]])=>{
        for((groupkey,innerMap)<-map2){
          for((key,value)<-innerMap){
            val mapInner = map1.getOrElseUpdate(groupkey, new mutable.HashMap[String,Double]() )
            mapInner.put(key,value+mapInner.getOrElse(key,0.0))
          }
        }
        map1
      }
    ).map{ case( groupKey:String,keyCountMap:mutable.HashMap[String,Double])=>
      ${standardDistribute}.keySet.filter(keyCountMap.keySet.contains(_)==false).foreach(keyCountMap.put(_,0.0))  //补足

      var total = keyCountMap.values.sum + 0.001
      if (${dividend}=="first")
        total = keyCountMap.toArray.sortBy(_._1).head._2
      val realRatioDistribute = keyCountMap.toArray.sortBy(_._1).map { case (key, value) => (key,value/total) }
      var diff = getDifference(${standardDistribute},realRatioDistribute,groupKey,barInfo)
      var relativeEntropyScore = new RelativeEntropy(${standardDistribute}.toArray.sortBy(_._1).map(_._2.head),keyCountMap.toArray.sortBy(_._1).map(_._2.toDouble)).score()
      var relativeEntropyScore2 = 0.0
      if (${standardDistributeSecond}.nonEmpty){
        relativeEntropyScore2 = new RelativeEntropy(${standardDistributeSecond}.toArray.sortBy(_._1).map(_._2.head),keyCountMap.toArray.sortBy(_._1).map(_._2.toDouble)).score()
        if (relativeEntropyScore2 < relativeEntropyScore){
          relativeEntropyScore = relativeEntropyScore2
          diff = getDifference(${standardDistributeSecond},realRatioDistribute,groupKey,barInfo)
        }
      }
      val refined = keyCountMap.toArray.filter(_._2 > 0).sortBy(_._2).reverse.map { case (key, value) => s"${key}:${value.toInt}" }.mkString("|")

      val realRatioDistributeFile = realRatioDistribute.filter(_._2 > 0).sortBy(_._2).reverse.map { case (key, value) => "%s:%.3f".format(key,value) }.mkString("|")
      if(keyCountMap.values.sum<${statLowerLimits}){
        new RelativeEntropyDesc(groupKey,0.0.toString
          , refined, Map[String,Double](),realRatioDistributeFile)
      }else {
        new RelativeEntropyDesc(groupKey, "%.3f".format(relativeEntropyScore)
          , refined, diff,realRatioDistributeFile)
      }
    }.toArray

    //    val statInfo = data.rdd.groupBy(AntiSpamAlgorithm.getStatKey(statDims,_))
    //      .map{case (key,iter)=>
    //        var keyCountMap = new mutable.HashMap[String,Double]()
    //        iter.foreach{row=>
    //          val miniStatStr = row.getAs[String](${inputCols}(0))
    //          miniStatStr.split("\\|").filter{str=>str.length>=3&&str.contains(":")}.map{ str=>
    //            val splits = str.split(":")
    //            (splits(0),splits(1))
    //          }.foreach{case (key,value)=> keyCountMap.put(key,value.toDouble+keyCountMap.getOrElse(key,0.0))}
    //        }
    //        broadcastStandard.value.keySet.filter(keyCountMap.keySet.contains(_)==false).foreach(keyCountMap.put(_,0.0))  //补足
    //        val diff = getDifference(broadcastStandard.value,keyCountMap)
    //        val relativeEntropy = new RelativeEntropy(broadcastStandard.value.toArray.sortBy(_._1).map(_._2.toDouble),keyCountMap.toArray.sortBy(_._1).map(_._2.toDouble))
    //        if(keyCountMap.values.sum<${statLowerLimits}){
    //          new RelativeEntropyDesc(key,0.0.toString
    //            , keyCountMap.toArray.filter(_._2 > 0).sortBy(_._2).reverse.map { case (key, value) => s"${key}:${value.toInt}" }.mkString("|")
    //            , Map[String,Double]())
    //        }else {
    //          new RelativeEntropyDesc(key, "%.3f".format(relativeEntropy.score())
    //            , keyCountMap.toArray.filter(_._2 > 0).sortBy(_._2).reverse.map { case (key, value) => s"${key}:${value.toInt}" }.mkString("|")
    //            , diff)
    //        }
    //      }.collect()
    copyValues(new DistributeFeatureModel(uid,statInfo).setParent(this))
  }

  override def copy(extra: ParamMap): Estimator[DistributeFeatureModel] = {
    defaultCopy(extra)
  }
  override def transformSchema(schema: StructType): StructType = {
    super.checkSchema(schema)
  }
}