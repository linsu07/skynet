package org.apache.spark.ml.pc.features

import com.typesafe.config.Config
import org.apache.spark.ml.common.{AntiSpamModel, CheckStatus, ModelPath}
import org.apache.spark.ml.param.{BooleanParam, IntParam, ParamMap, Params, StringArrayParam}
import org.apache.spark.ml.util.{DefaultParamsReader, DefaultParamsWritable, DefaultParamsWriter, MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
trait UserActionParam extends Params{
  val includeThinktime = new BooleanParam(this,"includeThinktime","")
  val vocabulary = new StringArrayParam(this,"vocabulary","")
}

class UserActionIndexModel  (override val uid: String) extends AntiSpamModel[UserActionIndexModel]
  with UserActionParam with MLWritable with DefaultParamsWritable{
  override def setConfig(c: Config): this.type = {
    super.setConfig(c)
    set(includeThinktime->c.getBoolean("includeThinktime"))
    set(vocabulary->c.getStringList("vocabulary").asScala.toArray)
    this
  }

  override def copy(extra: ParamMap): UserActionIndexModel = {
    copyValues(new UserActionIndexModel(uid).setParent(parent), extra)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val map = ${vocabulary}.map{s=>
      val splits=s.split(":")
      (splits(0),splits(1).toInt)
    }.toMap
    val toStrArray = udf{
      (actionList:Seq[String],timeListOri:Seq[String])=>
        val timeList = timeListOri.map(_.toLong)
        if(actionList.length==0){
          Array[String]()
        }else if(actionList.length==1){
          val ret = new ListBuffer[String]()
          ret.append("start")
          ret.appendAll(actionList)
          ret.append("end")
          ret.toArray
        }
        else {
          val timeList2 = new ListBuffer[Long]()
          timeList2.appendAll(timeList)
          val head = timeList2.remove(0)
          timeList2.append(head)
          // 1秒以内是-1
          // 6s 以内是0
          // 30s 是1
          // 180 s 是2
          // 24* 分钟 是 3
          // 120 分钟 是 4
          // 12 小时 是 5
          var lst = actionList.zip(timeList.zip(timeList2)).flatMap{case (action,(t1,t2))=>
            var peroid = {math.log(math.abs(t2-t1)/1000)/math.log(6)}.toInt
            if(peroid<0)
              peroid = -1
            if(${includeThinktime})
              Array(action,"t%d".format(math.min(peroid,5)))
            else
              Array(action)
          }
          if(${includeThinktime})
            lst = lst.take(lst.length-1) // 最后的时间是不需要的
          val ret = new ListBuffer[String]()
          ret.append("start")
          ret.appendAll(lst)
          ret.append("end")
          ret.toArray
        }
//        strs.map(broadcatVoc.value.get(_).get)
    }
    val fr = dataset.withColumn(${outputCols}(0),toStrArray(col(${inputCols}(0)),col(${inputCols}(1))))
    val broadcatVoc = dataset.sparkSession.sparkContext.broadcast(map)
    val toIdsArray = udf{
      (strs:Seq[String])=>
        strs.map(broadcatVoc.value.get(_).get)
    }
    fr.withColumn(${outputCols}(1),toIdsArray(col(${outputCols}(0))))
  }
}
