package org.apache.spark.ml.algo.distance

import net.qihoo.antispam.application.common.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object EuDistanceTest {
  val sep = ","
  //基础数据列
 // val keyinputs = Array("site", "SPV", "srcg", "ad_click_id", "ad_cheat_type", "antispam_label")
  val keyinputs = Array("site", "SPV", "srcg", "ad_click_id")
  //特征列
  val feainputs = Array("actionsCountScore","actionsTimeGapsScore","posClickScore","ADClickPosScore","ADClickViewGapScore","secondSearchScore")
  //val feainputs = Array("actionsCountScore","ActionEntropyScore","actionsTimeGapsScore","posClickScore","ADClickPosScore","ADClickViewGapScore","secondSearchScore")
  //训练输出数据
  val trainOutputPath = "/home/hdp-saonline-w/searchvalue/ad/algoModelStandard/EuDistanceModel"
  //预测输入数据
  //val predictInputPath = "/home/hdp-saonline-w/test_qs/adRes_f_j/pcsearchbox2.5.1/20200804/model/check_judge_group_info"
  //预测输出数据
  //val predictOutputPath = "/home/hdp-saonline-w/pcad/eu/adRes_f_j_20200804_22_23_24_25"

  /**
   * 预测结果和规则模型进行对比
   *
   * @param data
   * @param outputpath
   */
  def compareResult(data: DataFrame, outputpath: String) = {
    val needdata = data
      .select("ad_click_id", "ad_cheat_type", "antispam_label", "EuDistance").cache()
    val resList = new ListBuffer[(String, String, String, String, String, String, String, String)]

    for (bin <- 0.0.until(120.0).by(0.2)) {
      val nextBin = bin + 0.2
      val binDf = needdata.filter(needdata("EuDistance") >= bin && needdata("EuDistance") < nextBin)
      val binCount = binDf.count().toString
      val syhInBin = binDf.filter("ad_cheat_type>1").count().toString
      val rulesInBin = binDf.filter("antispam_label==1").count().toString
      val binIndepend4Syh = binDf.filter("ad_cheat_type<2").count().toString
      val binIndepend4Rules = binDf.filter("antispam_label==0").count().toString
      val binIndepend = binDf.filter("ad_cheat_type<2").filter("antispam_label==0").count().toString
      resList.append(("EuDistance", bin.toString, binCount, syhInBin, rulesInBin, binIndepend4Syh, binIndepend4Rules, binIndepend))
    }
    val binDf = needdata.filter(needdata("EuDistance") >= 120.0)
    val binCount = binDf.count().toString
    val shyInBin = binDf.filter("ad_cheat_type>1").count().toString
    val rulesInBin = binDf.filter("antispam_label==1").count().toString
    val binIndepend4Syh = binDf.filter("ad_cheat_type<2").count().toString
    val binIndepend4Rules = binDf.filter("antispam_label==0").count().toString
    val binIndepend = binDf.filter("ad_cheat_type<2").filter("antispam_label==0").count().toString
    resList.append(("EuDistance", "120.0", binCount, shyInBin, rulesInBin, binIndepend4Syh, binIndepend4Rules, binIndepend))

    val spark = SparkSession.builder().getOrCreate()
    spark.createDataFrame(resList).toDF("alogoName", "bin", "binCount", "shyInBin", "rulesInBin", "binIndepend4Syh", "binIndepend4Rules", "binIndepend")
      .repartition(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputpath)
  }
  /**
   * 预测结果和规则模型进行对比
   *
   * @param data
   * @param outputpath
   */
  def compareResultNew(data: DataFrame, outputpath: String) = {
    val needdata = data
      .select("ad_click_id", "EuDistance").cache()
    val resList = new ListBuffer[(String, String, String)]

    for (bin <- 0.0.until(100.0).by(0.2)) {
      val nextBin = bin + 0.2
      val binDf = needdata.filter(needdata("EuDistance") >= bin && needdata("EuDistance") < nextBin)
      val binCount = binDf.count().toString
      resList.append(("EuDistance", bin.toString, binCount))
    }
    val binDf = needdata.filter(needdata("EuDistance") >= 100.0)
    val binCount = binDf.count().toString
    resList.append(("EuDistance", "100.0", binCount))

    val spark = SparkSession.builder().getOrCreate()
    spark.createDataFrame(resList).toDF("alogoName", "bin", "binCount")
      .repartition(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputpath)
  }

  def main(args: Array[String]): Unit = {

    val args_map = Utils.parseArgs(args)
    val predictInputPath = args_map("predictInputPath")
    val predictOutputPath = args_map("predictOutputPath")

    val spark = SparkSession.builder().getOrCreate()
    //根据Check字段得到是否作弊
    val calLabel = udf {
      (str: String) =>
        var label = 0
        val arr = str.split(sep).map(_.toInt)
        if (arr.contains(5) || arr.filter(k => k == 4).length >= 3)
          label = 1
        label
    }


    //------*模型训练阶段*------
//    //参数初始化
//    val eu = new EuDistance()
//    eu.set(eu.inputCols, feainputs)
//    eu.set(eu.outputCols, Array("EuDistance"))
//    eu.set(eu.outputPath, trainOutputPath)
//    //训练输入数据
//    var trainInputDs: Dataset[Row] = null
//    for (i <- 20200722 to 20200725) {
//      val trainInputPath = "/home/hdp-saonline-w/test_likai/nature4maha/pcsearchbox2.5.1/" + i + "/model/check_judge_group_info"
//      val trainDf = spark.read.parquet(trainInputPath)
//      val middleDf = trainDf
//        //.withColumn("antispam_label", calLabel(concat_ws(sep, trainDf.columns.filter(k => k.contains("Check")).map(k => col(k)): _*)))
//        .select((keyinputs ++ feainputs).map(k => col(k)): _*)
//        //过滤中间量级渠道sample0.3
//        .filter(trainDf("SPV") >= 5000 && trainDf("SPV") <= 100000).sample(0.2)
//      if (i == 20200722) {
//        trainInputDs = middleDf
//      } else {
//        trainInputDs = trainInputDs.union(middleDf)
//      }
//    }
//    println("输入数据行数为：", trainInputDs.count())
//    //训练马氏距离模型并保存mv
//    //val euModel = eu.fit(trainInputDs)
//    eu.fit(trainInputDs)
    //------*模型训练阶段*------


    //------*模型预测阶段*------
    //获取模型
    val euModel = EuDistanceModel.load(trainOutputPath)
    //读取预测数据
    val predictDf = spark.read.parquet(predictInputPath)
    val predictMiddleDF = predictDf
      //.withColumn("antispam_label", calLabel(concat_ws(sep, predictDf.columns.filter(k => k.contains("Check")).map(k => col(k)): _*).as("value")))
      .select((keyinputs ++ feainputs).map(k => col(k)): _*)
    //预测结果
    val predictresDF = euModel.transform(predictMiddleDF)

    //存储结果明细数据得到每行数据的距离
    predictresDF.repartition(1)
      .write.mode("overwrite").option("header", true)
      .csv(predictOutputPath + "\\result")
    //预测结果和规则模型进行对比
    compareResultNew(predictresDF, predictOutputPath + "\\compareresult")

    //------*模型预测阶段*------


    spark.close()
  }
}
