package org.apache.spark.ml.ad.entropy

import com.typesafe.config.Config
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.algo.InfoEntropy
import org.apache.spark.ml.common.AntiSpamAlgorithm
import org.apache.spark.ml.param.{BooleanParam, DoubleParam, ParamMap, Params}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
trait GeneralEntropyParam extends Params{
  val isNormed = new BooleanParam(this,"isNormed","is calculate for normd entropy or not")
  val threshold = new DoubleParam(this,"threshold","")
}
class GeneralEntropy(override val uid:String) extends Estimator[GeneralEntropyModel]
  with AntiSpamAlgorithm with GeneralEntropyParam{
  override def setConfig(c: Config): GeneralEntropy.this.type = {
    super.setConfig(c)
    if(c.hasPath("isNormed"))
      set(isNormed->c.getBoolean("isNormed"))
    else
      set(isNormed->false)
    if(c.hasPath("threshold"))
      set(threshold->c.getDouble("threshold"))
    else
      set(threshold->1.0)
    this
  }
  override def fit(dataset: Dataset[_]): GeneralEntropyModel = {
    val anomalys = dataset.select(${inputCols}(0),${inputCols}(1)).rdd.map{ row =>
      val chl = row.getString(0)
      val stat = row.getString(1)
      val statMap = stat.split("\\|").map{kv=>
        val splits = kv.split(":")
        (splits(0),splits(1).toInt)
      }.toMap
      val retMap = mutable.HashMap[String,Double]()
      if(stat.contains(":")==false || statMap.size==0 || statMap.values.sum<${statLowerLimits})
        (chl,retMap)
      else{
        var list = statMap.toArray.sortBy(_._2).reverse
        val entropy = new InfoEntropy()
        var entropyValue = 100000.0 // very big
        do{
          entropyValue = {
            if(${isNormed})
              entropy.getNormEntropy(list.map(_._2))
            else
              entropy.getEntropy(list.map(_._2))
          }
          if(entropyValue<${threshold})
            retMap.put(list.head._1,entropyValue)
          list = list.drop(1)
        }while(list.map(_._2).sum>${statLowerLimits}&&entropyValue<${threshold})
        (chl,retMap)
      }
    }.filter(_._2.size>0).collect().toMap
    copyValues(new GeneralEntropyModel(uid,anomalys))
  }

  override def copy(extra: ParamMap): Estimator[GeneralEntropyModel] = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    super.checkSchema(schema)
  }
}
