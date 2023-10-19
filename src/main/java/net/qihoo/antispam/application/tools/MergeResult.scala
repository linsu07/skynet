package net.qihoo.antispam.application.tools

import com.typesafe.config.{Config, ConfigFactory}
import net.qihoo.antispam.application.common.{GeneralApp, PathHelper}
import org.apache.spark.ml.common.Utils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

import scala.collection.JavaConverters._
import scala.util.control.Breaks._
object MergeResult extends GeneralApp {
  var start:String = _
  var end:String = _
  var outputRoot:String = _
  var sourcepath:String = _
  var spark:SparkSession = _
  var joinPath:String = _
  def merge(config: Config) = {
    val fileName = config.getString("mergedName")
    val source = config.getString("source")
    val days = end.toInt-start.toInt
    var frame:DataFrame = null
    breakable{
      for(i<-0 to days){
        val cur = Utils.getAnotherYYYYMMDD(start,i)
        val sourcePath = PathHelper.getPath(sourcepath,source,cur)
        var ret = spark.read.option("header","true").csv(sourcePath)
//        if(source.endsWith("cm.sheets_temp")){
//          val oldpath = PathHelper.getPath(joinPath,cur,"srcg")
//          val oldret = spark.read.option("header","true").csv(oldpath)
//          ret = ret.join(oldret,"channel")
//        }
        ret = ret.withColumn("date",lit(cur))
//        println("ret schema")
//        println(ret.schema)
        //.select("channel","pvAfterStar","pvAfterCtr","pvFinal")
        if(frame==null)
          frame = ret
        else
          frame = frame.union(ret)
        println("frame schema")
        println(frame.schema)

        if(cur.equals(end))
          break()
      }
    }
    val outputPath = PathHelper.getPath(outputRoot,fileName)
    frame.repartition(1).write.mode(SaveMode.Overwrite).option("header","true").csv(outputPath)
//    if (source.contains("rules.chl.statistics"))
//    {
//      val data = frame.rdd
//        .map(line=>((line.getString(0),line.getString(1)),(line.getString(2).toDouble+line.getString(3).toDouble+line.getString(4).toDouble+line.getString(5).toDouble+line.getString(6).toDouble+ line.getString(7).toDouble, line.getString(7).toDouble)))
//        .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(line=>{
//        val ((channel,algorithm),(total,spam))= line
//        (channel,algorithm,total,spam,spam/total)
//      }).filter(line=>line._5 >0).collect().toSeq
//
//      val spamoutputPath = outputPath+".spaminfo"
//      spark.createDataFrame(data).toDF("channel","algorithm","total_pv","spam","rate")
//        .repartition(1).write.mode(SaveMode.Overwrite).option("header","true").csv(spamoutputPath)
//    }
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("MergeResult.conf")
    val isLocal = config.getString("os.name").contains("Window")
    println("os %s ,isLocal %s" format(config.getString("os.name"), isLocal))
    val appname = config.getString("app_name")
    spark = createSpark(isLocal, appname)

    start = config.getString("start")
    end = config.getString("end")
    outputRoot = config.getString("output")
    sourcepath = config.getString("sourcepath")
//    joinPath = config.getString("joinpath")
    if(end.toInt<=start.toInt)
      throw new Exception(s"start ${start} can not bigger than ${end}")

    val taskList = config.getConfigList("task_list").asScala
    taskList.foreach(merge(_))

    spark.close()
  }

  override def start(spark: SparkSession, data: DataFrame): DataFrame = ???
}
