package org.apache.spark.ml.common

import java.util.Date

import com.typesafe.config.Config
import net.qihoo.antispam.application.common.PathHelper
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object AntiSpamAlgorithm{
  val statColName = "stat_map"
  def getName(config: Config):String={
    var name:String = "unk"
    if(config.hasPath("name"))
      name = config.getString("name")
    else{
      val className = config.getString("class")
      name = className.substring(className.lastIndexOf(".")+1)
      if(config.hasPath("statDimension")) {
        val statdims = new ListBuffer[String]()
        if(config.hasPath("hiddenDims"))
          statdims++=config.getStringList("hiddenDims").asScala
        else
          statdims++=config.getStringList("statDimension").asScala
        val suffix = statdims.mkString(Utils.sep)
        name = "%sBy%s".format(name,suffix)
      }
    }

    name
  }


  def getStatKey(dims:Array[String], row:Row):String={
    dims.map{ dim=>
        row.getAs[String](dim)
    }.mkString(Utils.sep)
  }
}

trait AntiSpamAlgorithm extends Params{
  var stageId:Int = _
  def setStageId(i: Int):Unit= {
    stageId = i
  }
  var context:Context = _
  def setConfig(c: Config):this.type = {
    var  config = c
    if(config.hasPath("inputCols"))
      set(inputCols->{
        val v = new ListBuffer[String]()
        v.appendAll(config.getStringList("inputCols").asScala)
        v.toArray
      })
    if(config.hasPath("inputFeaCols"))
      set(inputFeaCols->{
        val v = new ListBuffer[String]()
        v.appendAll(config.getStringList("inputFeaCols").asScala)
        v.toArray
      })
    if(config.hasPath("outputCols"))
      set(outputCols->{
        val v = new ListBuffer[String]()
        v.appendAll(config.getStringList("outputCols").asScala)
        v.toArray
      })
    if(config.hasPath("statDimension")) {
      val statdims = new ListBuffer[String]()
      statdims++=config.getStringList("statDimension").asScala
      set(statDimension -> statdims.toArray)
    }

    set(name->AntiSpamAlgorithm.getName(config))
//    set(scoreName->(${name}+"_score"))
    if(config.hasPath("statLowerLimits"))
      set(statLowerLimits->config.getInt("statLowerLimits"))
    else
      set(statLowerLimits->2000)


    if(config.hasPath("remit_list")){
      val l = new ListBuffer[String]()
      l++=config.getStringList("remit_list").asScala
      set(remitList->l.toArray)
    }
    if(config.hasPath("hiddenDims")) {
      val statdims = new ListBuffer[String]()
      statdims ++= c.getStringList("hiddenDims").asScala
      set(hiddenDims -> statdims.toArray)
    }else
      set(hiddenDims -> Array())
    if(config.hasPath("isFeatureModel")){
      set(isFeatureModel->c.getBoolean("isFeatureModel"))
    }
    if(config.hasPath("isJudgeModel")){
      set(isJudgeModel->c.getBoolean("isJudgeModel"))
    }

    if(config.hasPath("siteCol")){
      set(siteCol->c.getString("siteCol"))
    }

    if(config.hasPath("scoreName"))
      set(scoreName->c.getString("scoreName"))
    if(config.hasPath("channelCol"))
      set(channelCol->c.getString("channelCol"))
    if(config.hasPath("isJudgeModel")){
      set(isJudgeModel->c.getBoolean("isJudgeModel"))
    }
    if(config.hasPath("trainSource")){
      set(trainSource->c.getString("trainSource"))
    }
    if(config.hasPath("transformTarget"))
      set(transformTarget->c.getStringList("transformTarget").asScala.toArray)

    if(isaJudgeModel()&&config.hasPath("antiSpamType")) {
      val ty  =  c.getString("antiSpamType")
      val a = RuleType.withName(ty)
      set(antiSpamType ->a.toString)
    }
    this
  }
  def setContext(c:Context)={
    this.context = c
    set(modelPath->context.modelPath)
    set(day->context.curDay)
    set(standardModelPath->context.standardModelPath)
    this
  }
  val name = new Param[String](this,"name","every algorithm must have a unique name")
  val scoreName = new Param[String](this,"scoreName","feature输出列名")
  val inputCols = new StringArrayParam(this,"inputCols","cols need to convert")
  val inputFeaCols = new StringArrayParam(this,"inputFeaCols","cols need to convert",ParamValidators.arrayLengthGt(0))
  val outputCols = new StringArrayParam(this,"outputCols","cols need to convert",ParamValidators.arrayLengthGt(0))
  val statDimension =  new StringArrayParam(this,"statDimension","every algorithm must have a Dimension for statistics, row means no statistic")
  val statLowerLimits = new IntParam(this,"statLowerLimits","每个统计维度最小的进入结果的统计量，少于这个值被认为没有统计意义")
  val remitList = new StringArrayParam(this,"remit_list" ,"此策略被豁免的名单")
  val hiddenDims = new StringArrayParam(this,"hiddenDims","real dimentions on stat")
  val modelPath = new Param[String](this,  "modelPath",  "")
  val day = new Param[String](this,  "day",  "")

  val isFeatureModel = new BooleanParam(this, "isFeatureModel","本算法是否当前是计算feature")
  // 确定模型是加判别列， 特征列和判别列可以同时出现
  // 特征列可以是多列，在outputcols指定， 判别列的列名就是算法的name，不用指定，只能是一列
  val isJudgeModel = new BooleanParam(this, "isJudgeModel","本算法是否当前是计算判断结果")
  //确定模型训练来源是global 还是local
  val trainSource = new Param[String](this,"trainSource","训练数据来源",ParamValidators.inArray(Array("global","local","major","globalDirFit")))
  // 确定模型tranform的目标是global还是local
  val transformTarget = new StringArrayParam(this,"transformTarget","transform数据目标")
  val standardModelPath = new Param[String](this,"standardModelPath","标准模型所在的位置")
  val channelCol = new Param[String](this,"channelCol","输入列中渠道的列名")
  val antiSpamType = new Param[String](this,"antiSpamType", "反作弊策略类型描述")
  val siteCol = new Param[String](this,"siteCol", "输入列中渠道的站点的列名")


  def getSandardModelPath()={
    ${standardModelPath}
  }

  setDefault(
    name->"undefined"
    ,inputCols->Array("undefined")
    ,outputCols->Array("undefined")
    ,inputFeaCols->Array("undefined")
    ,statDimension->Array[String]()
    ,remitList->Array()
    ,isFeatureModel->false
    ,isJudgeModel->true
    ,channelCol->"srcg"
    ,siteCol ->"site"
    ,trainSource->"global"
    ,transformTarget->Array("global")
    ,antiSpamType->RuleType.distribute.toString
  )
  def isaFeatureModel():Boolean={
    ${isFeatureModel}
  }
  def isaJudgeModel():Boolean = {
    ${isJudgeModel}
  }
  def getTrainSource()={
    ${trainSource}
  }
  def getTransformTarget()={
    ${transformTarget}
  }
  def setMode(isJudage:Boolean = true,isFeature:Boolean = false): AntiSpamAlgorithm ={
    set(isFeatureModel->isFeature)
    set( isJudgeModel->isJudage)
    this
  }

  def getStandardModel():StandardModel={
    val path = PathHelper.mkPath(Array(${standardModelPath},${name}))
    StandardModel.load(path)
  }

  def getStatDimension()={
    ${statDimension}
  }
  def getInputCols() = {
    ${inputCols}
  }
  def getinputFeaCols() = {
    ${inputFeaCols}
  }
  def getOutputCols() = {
    ${outputCols}
  }
  def getName()={
    ${name}
  }
  def getHiddenDimension()={
    ${hiddenDims}
  }
  def getType={
    ${antiSpamType}
  }
  def checkSchema(schema: StructType): StructType = {
    for(col<-$(inputCols)) {
      if(col.equals("click_details"))
        SchemaUtils.checkColumnType(schema,col, ArrayType(StringType,true), "column %s must be included ".format(col))
      else if(col.equals("click_num"))
        SchemaUtils.checkNumericType(schema,col,"column %s must be included ".format(col))
      else
        SchemaUtils.checkColumnType(schema,col, StringType, "column %s must be included ".format(col))
      //      SchemaUtils.appendColumn(schema, "%s%s".format(${digitalPrefix},col), DoubleType)
    }
    for(col<-$(statDimension)) {
      SchemaUtils.checkColumnType(schema,col, StringType, "column %s must be included ".format(col))
      //      SchemaUtils.appendColumn(schema, "%s%s".format(${digitalPrefix},col), DoubleType)
    }
    schema
  }

  def getStatMap(statMap: mutable.HashMap[String,mutable.HashMap[String,mutable.HashMap[String,Double]]],itemName:String=null)={
    val myStatMap = statMap.getOrElseUpdate(${name},new mutable.HashMap[String,mutable.HashMap[String,Double]]())
    myStatMap.getOrElseUpdate(itemName,new mutable.HashMap[String,Double]())
  }

  def getSpamGroup(stat:Map[String,Vector],threshold:Double)= {
    stat.map{ case (groupKey,vector) =>
      if (vector.numActives > 0) {
        val sum = vector.toArray.sum
        val maxPos = vector.argmax
        if((vector.toArray(maxPos)/sum)>threshold)
          (groupKey,true,maxPos)
        else
          (groupKey,false,-1)
      } else
        (groupKey,false,-1)
    }.filter(_._2).map{ case(key,judge,pos)=>
      (key,pos)
    }.toMap
  }
}
