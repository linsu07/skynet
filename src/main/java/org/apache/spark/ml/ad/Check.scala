package org.apache.spark.ml.ad

import com.typesafe.config.Config
import org.apache.spark.ml.common.{AntiSpamModel, CheckStatus}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Random

trait CheckParam extends Params {
  val spamExpr = new Param[String](this,"spamexpr","定义条件")
  val anomalyExpr = new Param[String](this,"anomalyexpr","定义条件")

}
class Check(override val uid:String) extends AntiSpamModel[Check] with CheckParam{
  set(spamExpr->"")
  set(anomalyExpr->"")
  override def setConfig(c: Config): this.type = {
    super.setConfig(c)
    if(c.hasPath("spamExpr"))
     set(spamExpr->c.getString("spamExpr"))
    if(c.hasPath("anomalyExpr"))
     set(anomalyExpr->c.getString("anomalyExpr"))
    this
  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputs = ${outputCols}(0)
    val isRemitUdf = udf{
      (chl:String)=>
        if ($ {remitList}.length > 0 && regConain(${remitList},chl))
          true
        else
          false
    }
    if((${spamExpr}.length>0)&&(${anomalyExpr}.length>0))
      dataset.withColumn(${outputCols}(0),when(isRemitUdf(col(${channelCol})),CheckStatus.remit.id)
        .when(expr(${spamExpr}),CheckStatus.spam.id)
      .when(expr(${anomalyExpr}),CheckStatus.anomaly.id)
        .otherwise(CheckStatus.real.id))
    else if(${spamExpr}.length>0)
      dataset.withColumn(${outputCols}(0),when(isRemitUdf(col(${channelCol})),CheckStatus.remit.id)
        .when(expr(${spamExpr}),CheckStatus.spam.id)
        .otherwise(CheckStatus.real.id))
    else if(${anomalyExpr}.length>0)
      dataset.withColumn(${outputCols}(0),when(isRemitUdf(col(${channelCol})),CheckStatus.remit.id)
        .when(expr(${anomalyExpr}),CheckStatus.anomaly.id)
        .otherwise(CheckStatus.real.id))
    else
      throw new RuntimeException("spamexpr, anomalyexpr must be set, pls check")
  }
  override def copy(extra: ParamMap): Nothing = ???
}

object Check{
  def main(args: Array[String]): Unit = {
    val a = "ab#\r\n | :ab"
    println(a.replaceAll("[\\s|\\||\\#|\\:]*",""))
//    val spark = SparkSession.builder().appName("this")
//      .master("local").getOrCreate()
//
//    import spark.implicits._
//
//    val columns = Seq("name","address")
//    val data = Seq(("Robert, Smith", "1 Main st, Newark, NJ, 92537"),
//      ("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732"),
//        ("Alice, Garcia","3456 Walnut st, Newark, NJ, 94732")
//    )
//    var dfFromData = spark.createDataFrame(data).toDF(columns:_*)
//
//    val newDF = dfFromData.map(f=>{
//      val nameSplit = f.getAs[String](0).split(",")
//      val addSplit = f.getAs[String](1).split(",")
//      (nameSplit(0),nameSplit(1),addSplit(0),addSplit(1),addSplit(2),addSplit(3))
//    })
//    var finalDF = newDF.toDF("FirstName","LastName",
//      "Address Line1","City","State","zipCode")
//    finalDF.printSchema()
////    finalDF = finalDF.withColumn("check",when(expr("FirstName='Robert'"),true)
////    .or(when(expr("FirstName='Maria'"),1).otherwise(0)))
//
//    finalDF = finalDF.withColumn("check",when(expr("FirstName='Robert'"),CheckStatus.spam.id)
//      .when(expr("FirstName='Maria'"),CheckStatus.anomaly.id).otherwise(CheckStatus.real.id))
//    finalDF.show(false)
//    spark.close()

  }
}