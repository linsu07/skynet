package org.apache.spark.ml.common

import net.qihoo.antispam.application.common.PathHelper
import org.apache.hadoop.fs.{Hdfs, Path}
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus
import org.apache.spark.ml.Model
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, types}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.util.control.Breaks._
import scala.util.matching.Regex


/**
  * @author linsu
  * @create 2019/12/10 18:48
  */
abstract class AntiSpamModel[T<:Model[T]] extends Model[T] with AntiSpamAlgorithm{

  /**
    * 返回本model类型 的前几天的model 列表，列表按时间顺序由近到远
    * @param start 列表最近的一天离当前时间的天数， 默认为1
    * @param days  获取天数， 默认为7
    * @return
    */
  def getPreviousModels (start:Int = 1,days:Int = 7):Array[_] = {
    val timeArray = start until (start+days)
    val m = this.getClass.getMethod("load", classOf[String])
    timeArray.map{ gap=>
      val step = -gap
      val time = Utils.getAnotherYYYYMMDD(${day},step)
      var path = ${modelPath}.replace(${day},time)
      path = PathHelper.mkPath(Array(path,${name}))
      val model = {
        try {
          m.invoke(null, path)
        }catch {
          case e:Throwable=>
//            e.printStackTrace()
            null
        }
      }
      model
    }.toArray
  }

  def check(groupKey:Seq[Any],inputs:Seq[Any]):Int = {
    CheckStatus.real.id
  }
  def calValue(groupKey:Seq[Any],inputs:Seq[Any]):Vector={
    Vectors.dense(Array(0.0))
  }
  def regConain(l:Array[String], chl:String): Boolean ={
    breakable{
      for(s<-l){
        val regex = s.r
        if (regex.findFirstIn(chl) != None)
          return true
      }
    }
    false
  }
  val judge = udf{
    (groupkey:Row,chl:String,inputs:Row)=>{
      try {
        if ($ {remitList}.length > 0 && regConain(${remitList},chl))
          CheckStatus.remit.id
        else {
          check(groupkey.toSeq, inputs.toSeq)
        }
      }catch {
        case e:Throwable=>
          e.printStackTrace()
          CheckStatus.err.id
      }
    }
  }
  val cal = udf{
    (groupkey:Row,inputs:Row)=>{
      try {
        calValue(groupkey.toSeq,inputs.toSeq)
      }catch {
        case e:Throwable=>
          e.printStackTrace()
          Vectors.dense(Array(0.0))
      }
    }
  }

  def addBroadcast(spark:SparkSession)={

  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    println("transforming %s ...".format(${name}))
    addBroadcast(dataset.sparkSession)
    var dimCols = ${statDimension}.map(col)
    if (${hiddenDims}.length>0)
      dimCols = ${hiddenDims}.map(col)
    if(dimCols.length==0) // 只有model情况，走这个逻辑
      dimCols = Array(col(${channelCol}))
//    dataset.withColumns(Seq(${name},${name}+"Value"),Seq(judge(struct(dimCols:_*),col("srcg"),struct(${inputCols}.map(col):_*))
//    ,cal(struct(dimCols:_*),struct(${inputCols}.map(col):_*))))
    var ret:DataFrame = dataset.asInstanceOf[DataFrame]
    if(${isFeatureModel})
      ret = ret.withColumn(${scoreName},cal(struct(dimCols:_*),struct(${inputCols}.map(col):_*)))

    if(${isJudgeModel})
      ret = ret.withColumn(${name},judge(struct(dimCols:_*),col(${channelCol}),struct(${inputCols}.map(col):_*)))
    ret
  }
  override def transformSchema(schema: StructType): StructType = {
    super.checkSchema(schema)
  }
}
trait ModelPath {
  def getStatPath(parentPath:String): String = new Path(parentPath, "stats").toString
  def getStatPath(parentPath:String, childPath:String): String = {
    PathHelper.mkPath(Array(parentPath,childPath))
  }

  def splitKey(spark:SparkSession,ret: DataFrame, keys: Array[String]): DataFrame = {
    if (keys.length==1) {
      val firstCol = ret.schema.fields(0).name
      val renamed = ret.withColumnRenamed(firstCol,keys(0))
      return renamed
    }
    else{
      val firstCol = ret.schema.fields(0).name
      val restSchema = ret.schema.fields.filter(_.name.equals(firstCol)==false)
      val allfield = keys.map(new StructField(_,StringType,true))++restSchema
      var newSchema = new types.StructType(allfield)
      val newRdd = ret.rdd.map{ row=>
        val ret = new ListBuffer[Any]()
        ret.appendAll(row.getString(0).split(Utils.sep))
        ret.appendAll(row.toSeq.zipWithIndex.filter(_._2!=0).map(_._1))
        new GenericRow(ret.toArray).asInstanceOf[Row]
      }
      spark.createDataFrame(newRdd,newSchema)
    }
  }
}
