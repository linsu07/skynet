package org.apache.spark.ml.ad.entropy

import java.util

import com.google.gson.Gson
import org.apache.spark.ml.ad.InfoFromDetailArryJsonHelper
import org.apache.spark.ml.common.{AntiSpamModel, Utils}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.search.features.Hour
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class AdClickViewGap(override val uid:String) extends AntiSpamModel with Hour {
  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputCol = ${inputCols}(0).split("\\.")(0)
    val outputCol = ${outputCols}(0).split("\\.")
    val colName = outputCol(0)
    val keyName = outputCol(1)
    val stat = udf{
      (row: Row)=>
        //ad_clicknum
        val clickViewGapArray = InfoFromDetailArryJsonHelper.getFromRow(row,${inputCols}(0)).map(_.toString)
        val gapStat = clickViewGapArray.map{ gap=>
          var gapNumber =  try{gap.toDouble}
          catch {
            case e:Throwable=>20.0
          }
          if(gapNumber<1)
            gapNumber=1
          val gapString = math.log(gapNumber).round.toString
          val retMap = new util.HashMap[String,String]()
          retMap.put(keyName,gapString)
          retMap
        }.toArray
        new Gson().toJson(gapStat)
    }
    dataset.withColumn(colName,stat(struct(col(inputCol))))
  }
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): Nothing = ???
}