package org.apache.spark.ml.ad.entropy

import com.typesafe.config.Config
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.algo.InfoEntropy
import org.apache.spark.ml.common.{AntiSpamAlgorithm, CheckStatus, Utils}
import org.apache.spark.ml.param.{DoubleParam, ParamMap, ParamValidators, Params, StringArrayParam}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import scala.collection.JavaConverters._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
trait ActionEntropyParam extends Params {
//  val spamScoreThreshold = new DoubleParam(this,"spamScoreThreshold","",ParamValidators.gt(0.0))
  val anomalyScoreThreshold = new DoubleParam(this,"anomalyScoreThreshold","",ParamValidators.gt(0.0))
//  val entropyThreshold = new DoubleParam(this,"entropyThreshold","entropy greate than is an anomaly")
  val attentionActionList = new StringArrayParam(this,"attentionActionList","")
}
class ActionEntropy (override val uid:String) extends Estimator[ActionEntropyModel]
  with AntiSpamAlgorithm with ActionEntropyParam{
  override def setConfig(c: Config): ActionEntropy.this.type = {
    super.setConfig(c)
//    set(spamScoreThreshold->c.getDouble("spamScoreThreshold"))
    set(anomalyScoreThreshold->c.getDouble("anomalyScoreThreshold"))
    if(c.hasPath("attentionActionList"))
      set(attentionActionList->c.getStringList("attentionActionList").asScala.toArray)
    else
      set(attentionActionList->Array[String]())
//    if(c.hasPath("entropyThreshold"))
//      set(entropyThreshold->c.getDouble("entropyThreshold"))
//    else
//      set(entropyThreshold->0.6)
    this
  }
  override def fit(dataset: Dataset[_]): ActionEntropyModel = {
    val curDims  = getStatDimension().take(getStatDimension().length-1)
    val chlEntropy= dataset.asInstanceOf[DataFrame].rdd.keyBy{row=>AntiSpamAlgorithm.getStatKey(curDims,row)}
    .aggregateByKey(new mutable.HashMap[String,Int]())(
      (map:mutable.HashMap[String,Int], row:Row)=>{
        val key = row.getAs[Seq[String]](${inputCols}(0)).mkString("->")
        if(${attentionActionList}.length>0){
          if(${attentionActionList}.find(key.contains(_)).nonEmpty){
            map.put(key,1+map.getOrElse(key,0))
          }
        }else{
          map.put(key,1+map.getOrElse(key,0))
        }
        map
      },
      (map1:mutable.HashMap[String,Int], map2:mutable.HashMap[String,Int])=>{
        map2.foreach{case (key,num)=>
          map1.put(key,num+map1.getOrElse(key,0))
        }
        map1
      }
    ).map{ case (chl,map)=>
      val total=map.values.sum
      // 有的渠道行为种类过少， 所以熵计算有些失真，现保证行为个数必须有行为总个数的1/4
      val actionStatList = new ListBuffer[Int]()
      actionStatList.appendAll(map.values.toArray.sorted.reverse)
      val padding = {
        val padnum = math.max(0,(total/4)-actionStatList.length)
        Array.fill(padnum)(0)
      }
      actionStatList.appendAll(padding)
      val oriEntropy = new InfoEntropy().getNormEntropy(actionStatList.toArray)
      val anomalyValuesDelta = new ListBuffer[Double]()
      if(oriEntropy<${anomalyScoreThreshold}){
//        anomalyValuesDelta.append(${anomalyScoreThreshold}-oriEntropy)
        anomalyValuesDelta.appendAll(getAnomalyValues(actionStatList,oriEntropy))
      }
      val anomalyAction = map.toArray.sortBy(_._2).reverse.take(anomalyValuesDelta.length)
      (chl,(oriEntropy,total,map.values.size,anomalyAction,anomalyValuesDelta.toArray))
    }.collect().toMap

    copyValues(new ActionEntropyModel(uid)).setEntropyMap(chlEntropy)
  }

  def getAnomalyValues(list:ListBuffer[Int],firstEntropy:Double):ListBuffer[Double]={
    val ret = new ListBuffer[Double]()
    var entropy = firstEntropy
//    entropy = new InfoEntropy().getNormEntropy(list.toArray)
    while(entropy< ${anomalyScoreThreshold}){
      val delta = ${anomalyScoreThreshold}-entropy
      ret.append(delta)
      var size = math.max(1,ret.size/2)
      size = math.min(size,list.size)
      list.remove(0,size)
      entropy = new InfoEntropy().getNormEntropy(list.toArray)
    }
    ret
  }
  override def copy(extra: ParamMap): Estimator[ActionEntropyModel] = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    super.checkSchema(schema)
  }
}
