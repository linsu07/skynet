package org.apache.spark.ml.ad.entropy

/**
  *@author qinsha 20200806
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


class AdIndustryDistributeFeature(override val uid:String) extends org.apache.spark.ml.ad.entropy.DistributeFeature
{
  def getDifferenceNew(standard:Map[String,ListBuffer[Double]],realDistribute:mutable.HashMap[String,Double],groupKey:String, isBar:Boolean): Map[String,Double] ={
    var countSum:Double =  realDistribute.values.sum + RelativeEntropy.epsilon
    val real = realDistribute.map{case(key,value)=>(key,value/countSum)}.toArray.sortBy(_._1)
    standard.toArray.sortBy(_._1).zip(real).map{ case ((key,standRatio),(key2,realRatio))=>
      var resRatioStd = (realRatio-standRatio.head)/(standRatio.last + RelativeEntropy.epsilon)

      if(${remitKey}.nonEmpty && isBar && ${remitKey}.contains(key.toString))
      {
        resRatioStd = (realRatio-${remitKey}(key).head)/(${remitKey}(key).last + RelativeEntropy.epsilon)
      }
      (key,{if (resRatioStd>0) resRatioStd else 0.0})
    }.toMap
  }

  override def fit(dataset: Dataset[_]): DistributeFeatureModel = {
    //    val broadcastStandard = dataset.sparkSession.sparkContext.broadcast(${standardDistribute})
    val statDims: Array[String] = getStatDimension().take(getStatDimension().length-1)
    val specDim = getStatDimension().tail
    val data = dataset.asInstanceOf[DataFrame]
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
      val siteArray = Array("http://www.wb123.com","http://nav.bohaiqilin.com","https://www.52daohang.com","http://www.feihuo.com")
      val siteName = groupKey.split(Utils.sep)(statDims.indexOf("site"))
      val topCategory = keyCountMap.toSeq.sortBy(_._2).reverse.head._1
      var isBar = false
      if (siteArray.contains(siteName) && topCategory == "游戏")
        isBar = true

      ${standardDistribute}.keySet.filter(keyCountMap.keySet.contains(_)==false).foreach(keyCountMap.put(_,0.0))  //补足
    var diff = getDifferenceNew(${standardDistribute},keyCountMap,groupKey,isBar)
      var relativeEntropyScore = new RelativeEntropy(${standardDistribute}.toArray.sortBy(_._1).map(_._2.head),keyCountMap.toArray.sortBy(_._1).map(_._2.toDouble)).score()
      var relativeEntropyScore2 = 0.0
      if (${standardDistributeSecond}.nonEmpty){
        relativeEntropyScore2 = new RelativeEntropy(${standardDistributeSecond}.toArray.sortBy(_._1).map(_._2.head),keyCountMap.toArray.sortBy(_._1).map(_._2.toDouble)).score()
        if (relativeEntropyScore2 < relativeEntropyScore){
          relativeEntropyScore = relativeEntropyScore2
          diff = getDifferenceNew(${standardDistributeSecond},keyCountMap,groupKey,isBar)
        }
      }
      val refined = keyCountMap.toArray.filter(_._2 > 0).sortBy(_._2).reverse.map { case (key, value) => s"${key}:${value.toInt}" }.mkString("|")
      val total = keyCountMap.values.sum + 0.001
      val realRatioDistribute  = keyCountMap.toArray.filter(_._2 > 0).sortBy(_._2).reverse.map { case (key, value) => "%s:%.3f".format(key,value/total) }.mkString("|")
      if(keyCountMap.values.sum<${statLowerLimits}){
        new RelativeEntropyDesc(groupKey,0.0.toString
          , refined, Map[String,Double](),realRatioDistribute)
      }else {
        new RelativeEntropyDesc(groupKey, "%.3f".format(relativeEntropyScore)
          , refined, diff,realRatioDistribute)
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