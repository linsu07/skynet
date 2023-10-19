package org.apache.spark.ml.algo.density

import org.apache.spark.ml.common.AntiSpamModel
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.util.{MLReader, MLWritable}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.algo.density.IForestModel
import org.apache.spark.ml.common.Utils
import org.apache.spark.sql.functions.{col, expr, lit, mean, stddev, when,concat_ws,min,max}

object IForestTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").getOrCreate()
    //val addr = "D:\\work\\搜索反作弊事业部\\广告反作弊\\策略结果\\ad_pcsearchbox\\20200804_new_adpc\\model\\20200804_check_judge_group_info"
    var addr = "D:\\work\\搜索反作弊事业部\\广告反作弊\\策略结果\\ad_pcsearchbox\\iforesDATA\\adnature_20200720"
    addr = "D:\\work\\搜索反作弊事业部\\广告反作弊\\策略结果\\ad_pcsearchbox\\iforesDATA\\20200722_nature_check_judge_group_info"
    var addr2 = Array("D:\\work\\搜索反作弊事业部\\广告反作弊\\策略结果\\ad_pcsearchbox\\iforesDATA\\20200501\\model\\check_judge_group_info\\",
      "D:\\work\\搜索反作弊事业部\\广告反作弊\\策略结果\\ad_pcsearchbox\\iforesDATA\\20200502\\model\\check_judge_group_info\\",
      "D:\\work\\搜索反作弊事业部\\广告反作弊\\策略结果\\ad_pcsearchbox\\iforesDATA\\20200503\\model\\check_judge_group_info\\")
    val output = "D:\\work\\testdata\\iforest"

    // checkpath = "D:\\work\\搜索反作弊事业部\\广告反作弊\\策略结果\\ad_pcsearchbox\\iforesDATA\\adnature_20200720"



    val natureDF = spark.read.parquet(addr).filter("SPV<100000")
    val PCsearchDF = spark.read.parquet(addr2:_*)

    //println(PCsearchDF.count())



    def getTrainDF(natureDF:DataFrame,badPcDF:DataFrame,sampleRate:Double=0.2,featureList:Array[String]= Array[String]()) ={
      val frame_temp = natureDF.sample(sampleRate)
      val frame1 = GetNeedFeatureDeal.getClearData(frame_temp,true)
      //println(frame1.columns.mkString("|"))
      val frame_temp2 = GetNeedFeatureDeal.getBadTrainData(badPcDF,featureList).sample(0.35)
      val frame2 = GetNeedFeatureDeal.getClearData(frame_temp2,true)
      val frame = frame1.union(frame2)
      println(frame_temp.count(),frame1.count(), frame2.count(),frame.count())
      println(frame2.columns.mkString("|"))
      val badRate = frame2.count()/frame1.count().toDouble
      (frame.asInstanceOf[DataFrame],badRate)
    }

    //val featureList = Array("actionsCountScore","ActionEntropyScore","ipBinScore","qBinScore","adindustryScore","actionsTimeGapsScore","adClickHourScore","posClickScore","secondSearchScore","age0RateScore","ADCPV2ADSPVScore","CTRScore","ADCPV2ADCUVScore","sugSearchRateScore")
    val featureList = Array("actionsCountScore","ActionEntropyScore","actionsTimeGapsScore","posClickScore","ADClickPosScore","ADClickViewGapScore","secondSearchScore")

    var finalResDF = spark.emptyDataFrame
    var ScoreBinDF = spark.emptyDataFrame
    val tryRound = 6

    val day = "20200501"

    val tryType = "FinalCheck3_20200804"
    val outputSpecPath = output + s"\\ScoresSpecnBadAddNature_${tryType}_${day}_step_${tryRound}"
    val outputBinPath = output + s"\\ScoresBinInfoBadAddNature_${tryType}_${day}_step_${tryRound}"
    val outputModelPath = output + s"\\model_${tryType}_${day}_step_${tryRound}"

    val sampleRate = 0.30
    //val (frame,badRate) = getTrainDF(natureDF,PCsearchDF,sampleRate,featureList)
    //    println("badRate is ",badRate)
    //
    //    frame.select(featureList.map(k=>mean(when(col(k)>=0.0, col(k))).alias(k+"mean")): _*).show()
    //    frame.select(featureList.map(k=>min(when(col(k)>=0.0, col(k))).alias(k+"mean")): _*).show()
    //    frame.select(featureList.map(k=>max(when(col(k)>=0.0, col(k))).alias(k+"mean")): _*).show()

    //frame.write.option("header",true).mode("overwrite").save(output+"\\sample_0.08")

    val days = Array("20200502")
    // for (i<- 0.until(tryRound)) {
    for (day_temp <- days) {
      //      val day_temp = Utils.getAnotherYYYYMMDD(day,i)
      var checkpath = s"D:\\work\\搜索反作弊事业部\\广告反作弊\\策略结果\\ad_pcsearchbox\\iforesDATA\\$day_temp\\model\\check_judge_group_info\\"
      val checkDf = spark.read.parquet(checkpath)
      val checkframe = GetNeedFeatureDeal.getClearData(checkDf,true)

      //val badRate1 = (badRate*100).round
      //frame.repartition(1).write.mode("overwrite").option("header",true).csv(output+s"\\traindata_${tryType}_${day}_${sampleRate}")
      //val describe = s"day${day_temp}badrate${badRate1}"
      // val usedFeatureList = featureList
      //println(s"round $day_temp, des is ${describe}")
      //      val numTree = 100
      //      val MaxFeatures = 8
      //      val forest = new IForest()
      //      forest.set(forest.numTrees, numTree)
      //     // forest.setMaxSamples(maxSample)
      //      forest.setMaxFeatures(MaxFeatures)
      //      forest.setMaxDepth(7)
      //val columnName = describe + "tn" + numTree.toString + "maxF" + MaxFeatures.toString

      //val scoreColumnName = columnName+"Score"

      ///forest.set(forest.inputCols,usedFeatureList)
      //forest.set(forest.outputCols, Array(scoreColumnName))

      //val model = forest.fit(frame)
      //model.save(output+"model")
      //val model: IForestModel = IForestModel.load("D:\\work\\testdata\\iforest\\model_Final_20200525_step_5\\day20200525round1badrate8")


      val model: IForestModel = IForestModel.load("D:\\work\\testdata\\iforest\\model_final_adjust_20200825_step_3\\round2badrate8")

      val scoreColumnName =s"${day_temp}IFscores"
      model.set(model.outputCols, Array(scoreColumnName))
      println(scoreColumnName.toString())
      //model.asInstanceOf[MLWritable].write.overwrite().save(outputModelPath+s"\\${describe}")
      //val model: IForestModel = IForestModel.load(output+"\\model")

      val listtf = udf { str: DenseVector =>
        str.toArray.mkString("|")
      }

      val describe = day_temp

      val resDF = model.transform(checkframe).withColumn(describe+"features_list", listtf(col("iforest_features"))).drop("iforest_features")
      if (finalResDF.isEmpty)
      {
        finalResDF = resDF
        ScoreBinDF = GetNeedFeatureDeal.compareResult(resDF,scoreColumnName,0.002,0.2)
      }
      else
      {
        //val temp = resDF.select("ad_click_id",describe+"features_list",scoreColumnName)
        //finalResDF = finalResDF.join(temp,"ad_click_id")
        ScoreBinDF = ScoreBinDF.union(GetNeedFeatureDeal.compareResult(resDF,scoreColumnName,0.002,0.2))
      }
      ScoreBinDF.show
      resDF.repartition(1).write.option("header", true).mode("overwrite").csv(outputSpecPath+s"\\${describe}")
      finalResDF.repartition(1).write.option("header", true).mode("overwrite").csv(outputSpecPath)
      ScoreBinDF.repartition(1).write.option("header", true).mode("overwrite").csv(outputBinPath)
      //spark.sparkContext.clean(forest)
      //resDF.unpersist()
    }

    spark.close()
  }
}
