package org.apache.spark.ml.ad.entropy

/**
  *@author linsu at 2020/07/17
  */
import com.typesafe.config.Config
import org.apache.spark.ml.common.{AntiSpamAlgorithm, AntiSpamModel, Utils}
import org.apache.spark.ml.param.{DoubleParam, IntArrayParam, ParamMap, ParamValidators, Params}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}

import scala.collection.JavaConverters._


trait RedistributeByFrequencyParam extends Params {
  val binDesc = new IntArrayParam(this,"binDesc","",ParamValidators.arrayLengthGt(1.0))
  val unitSize = new DoubleParam(this,"unitSize","",ParamValidators.gt(999))
}
class RedistributeByFrequency (override val uid:String)extends AntiSpamModel[RedistributeByFrequency]
  with RedistributeByFrequencyParam{
  override def copy(extra: ParamMap): RedistributeByFrequency = ???

  override def setConfig(c: Config): RedistributeByFrequency.this.type = {
    set(binDesc->c.getStringList("binDesc").asScala.toArray.map(_.toInt))
    if(c.hasPath("unitSize"))
      set(unitSize->c.getDouble("unitSize"))
    else
      set(unitSize->5000)
    super.setConfig(c)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val binDescBroadcast = dataset.sparkSession.sparkContext.broadcast(${binDesc}.sorted)
    val majorDims = getStatDimension().take(getStatDimension().length-1)
    val guidBinDic = dataset.asInstanceOf[DataFrame].rdd
      .flatMap{row=>
        val major = AntiSpamAlgorithm.getStatKey(majorDims,row)
        val guid = AntiSpamAlgorithm.getStatKey(getStatDimension(),row)
        val miniStatStr = row.getAs[String](${inputCols}(0))
        val spv = { if(${inputCols}.length>1) row.getAs[Double](${inputCols}(1))else -1 }
        val miniStat=miniStatStr.split("\\|").filter(_.length>0).map{ str=>
          val splits = str.split(":")
          (splits(0),splits(1).toInt)
        }
        miniStat.map{case(key,count)=>(s"${major}#${key}",count,guid,spv)}
      }.groupBy(_._1)
      .map{case (key,iter)=>
        var count =0
        var spv = iter.head._4
        val buf = new ListBuffer[String]()
        iter.foreach{row=>count=count+row._2;buf.append(row._3)}
        (key,count,buf.toArray,spv)
      }.flatMap{case (key,count,guids,spv)=>
        var selectedBin = binDescBroadcast.value.last
        var scale = {if (spv!=(-1)) ${unitSize}/spv else 1}
        val reCount = (count*scale)
        breakable(
          for(bin<-binDescBroadcast.value){
          if(reCount<=bin.toDouble){
            selectedBin = bin
            break()
          }
        })
        guids.map((_,key,selectedBin))
      }.groupBy(_._1)
      .map{case (guid,iter)=>
        val splits = guid.split(Utils.sep,-1)
        val binMap = iter.map{row=>
          val key = row._2.split("#").last
          (key,row._3.toString)
        }.toMap
          val values = new ListBuffer[Any]()
          values.appendAll(splits)
          values.append(binMap)
          new GenericRow(values.toArray).asInstanceOf[Row]
      }
    var schema = new StructType()
    getStatDimension().foreach{str=>schema = schema.add(str,StringType)}
    schema = schema.add(${outputCols}(0),MapType(StringType,StringType))
    val joined = dataset.sparkSession.createDataFrame(guidBinDic,schema).cache()
    val ret = dataset.join(joined,getStatDimension(),"left_outer")
    val replaceByBin=udf{
      (miniStatStr:String,target:Map[String,String])=>
        val miniStat=miniStatStr.split("\\|").filter(_.length>0).map{ str=>
          val splits = str.split(":")
          (splits(0),splits(1).toInt)
        }
        miniStat.map{case (key,count)=>
          try {
            s"${target.get(key).get}:${count}"
          }catch{
            case e:Throwable=>
              println("the key is " + key)
              println(target.mkString("|"))
              "1:1"
          }
        }.mkString("|")
    }
    ret.withColumn(${outputCols}(0),replaceByBin(col(${inputCols}(0)),col(${outputCols}(0))))
  }
}
