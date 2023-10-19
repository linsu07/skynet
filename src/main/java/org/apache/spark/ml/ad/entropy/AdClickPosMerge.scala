package org.apache.spark.ml.ad.entropy

/**
  * @author linsu  at 2020/08/04
  */

import java.util

import com.google.gson.Gson
import org.apache.spark.ml.ad.InfoFromDetailArryJsonHelper
import org.apache.spark.ml.common.{AntiSpamModel, Utils}
import org.apache.spark.ml.param.ParamMap

import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
//"ads_details.location"
//"ads_details.pos"
//"ads_loc.full_location
class AdClickPosMerge(override val uid:String) extends AntiSpamModel with Hour {
  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputCol = ${inputCols}(0).split("\\.")(0)
    val outputCol = ${outputCols}(0).split("\\.")
    val colName = outputCol(0)
    val keyName = outputCol(1)
    val stat = udf{
      (row: Row)=>
        //ad_clicknum
        val clicknumArray = InfoFromDetailArryJsonHelper.getFromRow(row,${inputCols}(2))
        //"ads_details.ad_clickdetails.click_time"
        val locArray = InfoFromDetailArryJsonHelper.getFromRow(row,${inputCols}(0))
        val posArray = InfoFromDetailArryJsonHelper.getFromRow(row,${inputCols}(1))
        val locStat = new mutable.HashMap[String,Int]()
        val newLocations = locArray.zip(posArray).zip(clicknumArray).filter{ info =>
          val number = {
            try{info._2.toString.toInt}
            catch {
              case e:Throwable=>0
            }
          }
          number>0
        }.flatMap{ case((loc,pos),clickNum)=>
          val location = s"${loc}(${pos})"
          val arr = new ListBuffer[util.HashMap[String,String]]()
          for(_<- 0 until(clickNum.toString.toInt)){
            val retMap = new util.HashMap[String,String]()
            retMap.put(keyName,location)
            arr.append(retMap)
          }
          arr
        }.toArray
        new Gson().toJson(newLocations)
    }
    dataset.withColumn(colName,stat(struct(col(inputCol))))
  }
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): Nothing = ???
}