package org.apache.spark.ml.pc.ip

import com.typesafe.config.Config
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.common.{AggregateByKey, AntiSpamAlgorithm, CheckStatus, Utils}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class BlackIP(override val uid:String) extends Estimator[BlackIPModel]
  with AntiSpamAlgorithm   {
  def this() = this(Identifiable.randomUID("blackIP"))
  override def setConfig(c: Config): this.type = {
    super.setConfig(c)
    //    set(spamScoreThreshold->c.getDouble("spamScoreThreshold"))
    //    set(anomalyScoreThreshold->c.getDouble("anomalyScoreThreshold"))
    this
  }
  var algoNames :Array[String] = _
  def setAlgoNames(names:Array[String]) ={
    this.algoNames = names
  }
  override def fit(dataset: Dataset[_]): BlackIPModel = {
    val blackips = dataset.asInstanceOf[DataFrame].rdd.map{ row=>
      val key = ${inputCols}.map(row.getAs[String](_)).mkString(Utils.sep)
      val finalStatus = {
        val slist = row.getValuesMap[Int](algoNames).values.filter(_ >= CheckStatus.anomaly.id)
        if(slist.size==0)
          CheckStatus.real.id
        else if(slist.max==CheckStatus.spam.id)
          CheckStatus.spam.id
        else if(slist.size>=2)
          CheckStatus.spam.id
        else
          CheckStatus.real.id
      }
      val statMap = new mutable.HashMap[Int,Int]()
      statMap.put(finalStatus,1)
      (key,statMap)
    }.reduceByKey{(map1,map2)=>
      for((key,value)<-map2){
        map1.put(key,value+map1.getOrElse(key,0))
      }
      map1
    }.map{case (key,map)=>
        val prop = map.getOrElse(CheckStatus.spam.id,0).toDouble/map.values.sum.toDouble
        val isBlack = if(prop>0.5) true else false
      (key,isBlack)
    }.filter(_._2).map(_._1).collect().toSet

    copyValues(new BlackIPModel(uid,blackips).setParent(this))
  }
  override def copy(extra: ParamMap): Estimator[BlackIPModel] = {
    defaultCopy(extra)
  }


  override def transformSchema(schema: StructType): StructType = {
    super.checkSchema(schema)
  }
}
