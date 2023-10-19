package org.apache.spark.ml.pc.advertise

/**
  * @author qinsha
  * @create 2020/03/16
  *  功能： 查看广告反作弊判别情况，只判为疑似
  */

import com.typesafe.config.Config
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.common.{AggregateByKey, AntiSpamAlgorithm}
import org.apache.spark.ml.param.{DoubleParam, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.pc.AdclickHelper
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


trait AdAntispamgroupJudgeParam extends Params {
  val anomalyThreshold = new DoubleParam(this,"anomalyThreshold","",ParamValidators.gt(0.0))
}

class AdAntispamgroupJudge(override val uid:String) extends Estimator[AdAntispamgroupJudgeModel]
  with AntiSpamAlgorithm with AggregateByKey  with AdAntispamgroupJudgeParam{
  def this() = this(Identifiable.randomUID("AdAntispamgroupJudge"))
  val signItem = "antigroup_judge_info"
  val point_key = "judge"
  val spam_key ="spam"

  override def setConfig(c: Config): AdAntispamgroupJudge.this.type = {
    super.setConfig(c)
    set(anomalyThreshold->c.getDouble("anomalyThreshold"))
    this
  }

  override def fit(dataset: Dataset[_]): AdAntispamgroupJudgeModel = {
    val data = dataset.asInstanceOf[Dataset[(String,mutable.HashMap[String,mutable.HashMap[String,mutable.HashMap[String,Double]]])]]
    val unusualGroups = data.rdd.flatMap{ case(key,statMap)=>
      val buf = new ListBuffer[(String,Double,Double,Double)]()
      val statmap = getStatMap(statMap,signItem)
      val total = statmap.getOrElse(point_key,0.0)
      val spam_count = statmap.getOrElse(spam_key,0.0)
      val spam_rate = if (total !=0 ) spam_count/total else 0.0
      buf.append((key,total,spam_count,spam_rate))
      buf
    }.collect()
    copyValues(new AdAntispamgroupJudgeModel(uid,unusualGroups).setParent(this))
      .setanomalyMap(unusualGroups.filter(line=>line._2>${statLowerLimits} && line._4>=${anomalyThreshold}).map(_._1).toSet)
  }

  override def copy(extra: ParamMap): Estimator[AdAntispamgroupJudgeModel] = {
    defaultCopy(extra)
  }
  override def transformSchema(schema: StructType): StructType = {
    super.checkSchema(schema)
  }

  // 统计每个位置的点击个数
  override def seqOp(statMap:mutable.HashMap[String,mutable.HashMap[String,mutable.HashMap[String,Double]]], row: Row): mutable.HashMap[String,mutable.HashMap[String,mutable.HashMap[String,Double]]] = {
    val srcg = row.getAs[String](${inputCols}(0))
    val posMap = getStatMap(statMap, signItem)
    val adclick = AdclickHelper.getFromRow(row)
    if (adclick.length>0) {
      adclick.foreach { detail =>
        if (detail.spam.length > 0) {
          if (detail.spam == "1") {
            posMap.put(spam_key, detail.spam.toInt + posMap.getOrElse(spam_key, 0.0))
          }
          posMap.put(point_key, 1.0 + posMap.getOrElse(point_key, 0.0))
        }
      }
    }
    statMap
  }

  override def combOp(map1: mutable.HashMap[String,mutable.HashMap[String,mutable.HashMap[String,Double]]]
                      , map2: mutable.HashMap[String,mutable.HashMap[String,mutable.HashMap[String,Double]]])
  : mutable.HashMap[String,mutable.HashMap[String,mutable.HashMap[String,Double]]] = {
    val searchMap1 = getStatMap(map1,signItem)
    val searchMap2 = getStatMap(map2,signItem)
    for((pos,num)<-searchMap2)
      searchMap1.put(pos,num+searchMap1.getOrElse(pos,0.0))

    map1
  }
}
