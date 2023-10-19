package net.qihoo.antispam.application.pc.searchbox

import java.io.File
import java.util.concurrent.TimeUnit

import com.typesafe.config.{ConfigFactory, ConfigResolveOptions}
import net.qihoo.antispam.application.common.PathHelper


import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array_contains, col, lit, mean, stddev, udf, when}
import org.apache.spark.sql.types.StructType

/**
  * @author linsu at 2020/03/11 8:19
  */
object PcSearchBox extends PcSearchBoxShadow {
  def main (args: Array[String] ): Unit = {
    init(args)
    print(param.conf)
    config = ConfigFactory.parseFile(new File(param.conf)).resolve(
      ConfigResolveOptions.defaults().setUseSystemEnvironment(false)
    )
    val appname = config.getString("app_name")
    val isLocal = config.getBoolean("local")
    val spark = createSpark(isLocal,appname)
    var searchData = spark.read.parquet(param.searchPath)//.sample(0.1)

    try {
      start(spark, searchData)
    }catch {
      case e:Throwable=>
        throw new Exception("some Error occurred")
    }finally {
      executor.shutdown()
      executor.awaitTermination(20*60*1000, TimeUnit.MILLISECONDS) // wait 20分钟        executor.shutdown()
    }
    spark.close()
  }
  override def handleGroupFeature(gName: String, frame: DataFrame,judgeCols:Array[String]) = {
    println(s"${gName}'s frame is ...")
    var df = frame.drop("stat_map")

    val spark = SparkSession.builder().getOrCreate()
    val listtf = udf{ (str:Seq[String]) =>
      str.mkString("->")
    }
    if(gName.equals("srcg_guid_group")) df = df.where(array_contains(df("action_list"), "adclick"))
      .withColumn("action_time_list",listtf(col("action_time_list")))
      .withColumn("action_list",listtf(col("action_list")))
      .withColumn("time_list",listtf(col("time_list")))
      .withColumn("action_time_id_list",listtf(col("action_time_id_list")))
    else if (gName.equals("guid_group")) df = df.filter(col("AdclickNum") > 0)


    var df_new = df.drop(judgeCols:_*)
    if (gName.equals("srcg_group"))
      df_new = df_new.filter(df("SPV")>=5000)

//    val meanDf = df_new.select(df_new.columns.map(k=>mean(when(col(k)>0.0, col(k))).alias(k+"mean")): _*)
//    val stddevDf = df_new.select(df_new.columns.map(k=>stddev(when(col(k)>0.0, col(k))).alias(k+"stddev")): _*)

//    val rows = meanDf.rdd.zip(stddevDf.rdd).map{
//      case (rowLeft, rowRight) => Row.fromSeq(rowLeft.toSeq ++ rowRight.toSeq)}
//
//    // Merge schemas
//    val schema = StructType(meanDf.schema.fields ++ stddevDf.schema.fields)
    // Create new data frame
//    val finalDf =spark.createDataFrame(rows, schema)
//    finalDf.withColumn("day", lit(param.day))
//      .repartition(1)
//      .write.mode("overwrite").option("header",true)
//      .csv(PathHelper.getPath(param.modelPath,s"${gName}_featureFrame_mean_stddev"))
    df.withColumn("day", lit(param.day))
      .repartition(1)
      .write.mode("overwrite").option("header",true)
      .csv(PathHelper.getPath(param.modelPath,s"${gName}_featureFrame"))
  }
}

object pushAfterCheck extends PcSearchBoxShadow {
  def main (args: Array[String] ): Unit = {
    init (args)
    print (param.conf)
    config = ConfigFactory.parseFile (new File (param.conf) ).resolve (
    ConfigResolveOptions.defaults ().setUseSystemEnvironment (false)
    )
    val appname = config.getString ("app_name")
    val isLocal = config.getBoolean ("local")
    val spark = createSpark (isLocal, appname)
    copyTemp2Formal(spark,param.CMSheetPath)
    copyTemp2Formal(spark,param.starLevelPath)
    spark.close()
  }
}


