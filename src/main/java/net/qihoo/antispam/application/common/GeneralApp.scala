package net.qihoo.antispam.application.common

/**
  * @author linsu
  * @create 2019/12/10 18:48
  */
import com.typesafe.config.Config
import org.apache.spark.ml.common.Context
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

trait GeneralApp {
  var config: Config = _
  var context:Context = _
  var param:Param = _
  def createSpark(isLocal:Boolean,name:String):SparkSession = {
    if (isLocal)
      SparkSession.builder.master("local[5]").appName(name)
        .config("spark.sql.codegen.wholeStage", "false")
        .getOrCreate()
    else
      SparkSession.builder.appName(name)
        .config("spark.sql.codegen.wholeStage", "false")
        .getOrCreate()
  }
  def start(spark:SparkSession,data:DataFrame): DataFrame
  def copyTemp2Formal(spark:SparkSession,tempPath:String) = {
    val formalPath = tempPath.replace("_temp","")
    val sheet = spark.read.option("header","true").csv(tempPath)
    sheet.repartition(1).write.mode(SaveMode.Overwrite).option("header","true").csv(formalPath)
  }
}
