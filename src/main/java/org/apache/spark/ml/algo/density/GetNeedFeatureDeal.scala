package org.apache.spark.ml.algo.density

import net.qihoo.antispam.application.common.Utils
import net.qihoo.antispam.application.pc.ad.Advertising.param
import org.apache.spark
import org.apache.spark.sql.functions.{col, concat_ws, expr, lit, mean, stddev, when}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.SparkConf
import org.apache.spark.ml.common.Utils

import scala.collection.mutable.ListBuffer

/**
  * @Author qinsha
  * @create 2020/7/29 18:17
  */


object GetNeedFeatureDeal {
  def getClearData(dataOrigin:DataFrame,checkFlag:Boolean=false,usedFeatures:Array[String]=Array()): DataFrame = {
    //val spark = SparkSession.builder.master("local[3]").getOrCreate()
    //val spark = SparkSession.builder().appName("JoinAdResAndPcsearchbox").getOrCreate()

    val sep = ","
    var data = dataOrigin.asInstanceOf[DataFrame]
    val calLabel = udf {
      str: String =>
        var label = 0
        val arr = str.split(sep).map(_.toInt)
        if (arr.contains(5) || arr.count(k=>k==4) >=3)
          label = 1
        label
    }

    val totalClos = new ListBuffer[String]
    if(usedFeatures.nonEmpty)
      {
        totalClos.appendAll(usedFeatures)
      }
    else {
      val needCols = Array("ad_click_id", "site","srcg","ip", "src", "ua", "ad_advert_id", "ad_user_id", "ad_cprice", "ad_location", "ad_pos"
        , "ad_pn", "ad_clicknum", "ad_click_view_gap", "ad_press_duration", "hour", "age", "IPSeg", "browserName"
        , "minispv", "cpv", "adshow", "adclick")
      val numricClos_temp = data.columns.toList.filter(line => (line.contains("Count") || line.contains("Score")) && !line.contains("adClickIpInfoCount") && !line.contains("ipInfoCount")).filter(line => !line.contains("Check"))
      totalClos.appendAll(needCols)
      totalClos.appendAll(numricClos_temp)
    }
    totalClos.append("ad_cheat_type")
    if (checkFlag) {
      val newinfo = dataOrigin.withColumn("antispam_label", calLabel(concat_ws(sep, dataOrigin.columns.filter(k => k.contains("Check")).map(k => col(k)): _*).as("value")))
      //totalClos.append("antispam_label")

      totalClos.append("antispam_label")
      data = newinfo
    }

    val finalDf = data.select(totalClos.map(col(_)):_*)
    finalDf

  }

  def getBadTrainData(dataOrigin:DataFrame,featureList:Array[String]= Array[String]()): DataFrame ={
    val sep = ","
    var data = dataOrigin.asInstanceOf[DataFrame]
    val calLabel = udf {
      str: String =>
        var label = 0
        val arr = str.split(sep).map(_.toInt)
        if (arr.contains(5) || arr.count(k=>k==4) >=4)
          label = 1
        label
    }

    var newinfo = dataOrigin.withColumn("antispam_label", calLabel(concat_ws(sep, dataOrigin.columns.filter(k => k.contains("Score") && k.contains("Check")).map(k => col(k)): _*).as("value")))
    if (featureList.nonEmpty){
      println("herere")
      val checkList = featureList.map(_+"Check")
      newinfo = dataOrigin.withColumn("antispam_label", calLabel(concat_ws(sep, dataOrigin.columns.filter(k => checkList.contains(k)).map(k => col(k)): _*).as("value")))
    }


    newinfo.filter("antispam_label==1").drop("antispam_label")

  }


  def compareResult(data:DataFrame,scoreColumnName:String,binGap:Double=0.002,beginRate:Double=0.3,maxRate:Double=1.0)={
    val totalcount = data.count()
    val needdata = data.select("ad_click_id","ad_cheat_type","antispam_label",scoreColumnName).cache()
    val totalOnline = needdata.filter("ad_cheat_type>1").count().toString
    val totalAntispam = needdata.filter("antispam_label==1").count().toString
    val resList = new ListBuffer[(String,String,String,String,String,String,String,String,String,String,String)]

    val number = ((maxRate-beginRate)/binGap).toInt
    for (binIndex <- 1.until(number)){
      val bin = binGap*binIndex + beginRate
      val nextBin = bin+binGap
      val BinCount = needdata.filter(needdata(scoreColumnName)>=bin && needdata(scoreColumnName)<nextBin).count().toString
      val IForestCheck = needdata.filter(needdata(scoreColumnName)>=bin).cache()
      val iforestcount = IForestCheck.count()
      val OnineUiforest = IForestCheck.filter("ad_cheat_type>1").count().toString
      val AntispamUiforest  = IForestCheck.filter("antispam_label==1").count().toString
      val independ = IForestCheck.filter("ad_cheat_type<2").filter("antispam_label==0").count().toString
      val iforestCountrate = iforestcount.toDouble/totalcount
      resList.append((scoreColumnName,bin.toString,totalcount.toString,totalOnline,totalAntispam,iforestcount.toString,iforestCountrate.toString,BinCount,OnineUiforest,AntispamUiforest,independ))
    }


    val spark = SparkSession.builder().getOrCreate()
    val binDf = spark.createDataFrame(resList.toSeq).toDF("name","bininfo","totalclick","totalOnlineCount","totalAntispamCout","iforestCount","iforestCountRate","BinCount","OnlineUiForest","AntispamUiForest",
    "independ")

    binDf



  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").getOrCreate()
    val addr = "D:\\work\\搜索反作弊事业部\\广告反作弊\\策略结果\\ad_pcsearchbox\\20200804\\model\\check_judge_group_featureFrame"

    val frame_temp = spark.read.option("header",true).csv(addr)
    getClearData(frame_temp,true).show()

  }
}



