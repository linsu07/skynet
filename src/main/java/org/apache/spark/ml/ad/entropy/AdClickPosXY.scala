package org.apache.spark.ml.ad.entropy

import java.util

import com.google.gson.Gson
import com.typesafe.config.Config
import org.apache.spark.ml.ad.InfoFromDetailArryJsonHelper
import org.apache.spark.ml.common.{AntiSpamAlgorithm, AntiSpamModel, Utils}
import org.apache.spark.ml.param.{IntArrayParam, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.search.features.Hour
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

trait AdClickPosXYParam extends Params {
  val binDesc = new IntArrayParam(this,"binDesc","",ParamValidators.arrayLengthGt(1.0))
}
class AdClickPosXY(override val uid:String) extends AntiSpamModel[AdClickPosXY] with AdClickPosXYParam{
  override def setConfig(c: Config): AdClickPosXY.this.type = {
    super.setConfig(c)
    set(binDesc -> c.getStringList("binDesc").asScala.toArray.map(_.toInt))
    this
  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    val binDescBroadcast = dataset.sparkSession.sparkContext.broadcast(${binDesc}.sorted)
    val inputCol = ${inputCols}(0).split("\\.")(0)
    val outputCol = ${outputCols}(0).split("\\.")
    val colName = outputCol(0)
    val keyName = outputCol(1)
    def computeBin(cas:Double): Int ={
      var selectedBin = binDescBroadcast.value.last
      breakable(
        for(bin<-binDescBroadcast.value){
          if(cas<=bin.toDouble){
            selectedBin = bin
            break()
          }
        })
      selectedBin
    }
    val stat = udf{
      (row: Row)=>
        //ad_clicknum
        val adclickArray = InfoFromDetailArryJsonHelper.getFromRow(row,${inputCols}(0)).map(_.toString)
        val dataStat = adclickArray.filter(info => info.length>0 && Utils.isDigital(info)).map{ cas=>
          val str = computeBin(cas.toDouble).toString
          val retMap = new util.HashMap[String,String]()
          retMap.put(keyName,str)
          retMap
        }.toArray
        new Gson().toJson(dataStat)
    }
    dataset.withColumn(colName,stat(struct(col(inputCol))))
  }
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): Nothing = ???
}