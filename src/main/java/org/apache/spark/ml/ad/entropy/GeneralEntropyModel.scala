package org.apache.spark.ml.ad.entropy

import org.apache.spark.ml.common.AntiSpamModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable

class GeneralEntropyModel(override val uid:String,val anomalys:Map[String, mutable.HashMap[String, Double]]) extends AntiSpamModel[GeneralEntropyModel]
with GeneralEntropyParam {
  override def copy(extra: ParamMap): GeneralEntropyModel = {
    copyValues(new GeneralEntropyModel(uid,this.anomalys).setParent(parent), extra)
  }
  import org.apache.spark.sql.functions._
  override def transform(dataset: Dataset[_]): DataFrame = {
    val colNames = dataset.schema.fields.map(_.name)
    if(colNames.contains(${inputCols}(0))&&colNames.contains(${inputCols}(1))) { //chl group dataset
      val addStat = udf{
        (chl:String)=>
          if(anomalys.contains(chl)){
            anomalys.get(chl).get.toArray.map{case(key,value)=>
              val refined = "%.3f".format(value)
              s"${key}:${refined}"}.mkString("|")
          }else
            ""
      }
      dataset.withColumn(${outputCols}(0),addStat(col(${inputCols}(0))))
    }else if(colNames.contains(${inputCols}(2))){
      val addEntropy = udf{
        (chl:String,key:String)=>
          if(anomalys.contains(chl)){
            if(anomalys.get(chl).get.contains(key))
              anomalys.get(chl).get.get(key).get
            else
              100
          }else{
            100
          }
      }
      dataset.withColumn(${outputCols}(1),addEntropy(col(${inputCols}(0)),col(${inputCols}(2))))
    }else
      dataset.asInstanceOf[DataFrame]
  }
}
