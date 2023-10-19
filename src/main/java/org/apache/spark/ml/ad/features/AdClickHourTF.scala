package org.apache.spark.ml.ad.features

import java.util

import scala.collection.JavaConversions._
import com.google.gson.{Gson, JsonParser}
import org.apache.spark.ml.ad.InfoFromDetailArryJsonHelper
import org.apache.spark.ml.common.{AntiSpamModel, Utils}
import org.apache.spark.ml.common.Utils.pattern
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.search.features.Hour

class AdClickHourTF(override val uid:String) extends AntiSpamModel with Hour {
  override def transform(dataset: Dataset[_]): DataFrame = {
    val splits = ${outputCols}(0).split("\\.")
    val colName = splits(0)
    val keyName = splits(1)
    val inputCol = ${inputCols}(0).split("\\.")(0)
    case class hour()
    val addClickHour = udf{
      (row: Row)=>
        //"ads_details.ad_clickdetails.click_time"
        val adclicktimes = InfoFromDetailArryJsonHelper.getFromRow(row,${inputCols}(0))
        val timeJson = adclicktimes.toArray.filter(t=>Utils.isDigital(t.toString)).map{t=>
          val time = t.toString.toDouble.toLong.toString
          val hour = getHour(time)
          val map = new util.HashMap[String,String]()
          map.put(keyName,hour.toString)
          map
        }
        val ret = new Gson().toJson(timeJson)
        ret
//        val jsonArray = new JsonParser().parse(str).getAsJsonArray
//        jsonArray.foreach(info=>{
//          var arr = new util.ArrayList[String]()
//          var key = info.getAsJsonObject
//          val clicktime = key.get(${inputCols}(0)).getAsJsonArray
//          val showtime = key.get("show_time")
//          clicktime.foreach(subinfo=>{
//            var value = pattern.findAllIn(subinfo.toString.trim).mkString("")
//            if (value == "null"){
//              value = pattern.findAllIn(showtime.toString.trim).mkString("") + "000"
//            }
//            arr+=getHour(value)
//          })
//          key.add(${outputCols}(0), new Gson().toJsonTree(arr).getAsJsonArray)
//        })
//        jsonArray.toString
    }
    dataset.withColumn(colName,addClickHour(struct(col(inputCol))))
  }
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): Nothing = ???
}
