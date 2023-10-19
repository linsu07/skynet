package org.apache.spark.ml.ad.Groupfeatures

/**
  * @Author zhubaowen
  * @create 2020/7/10 19:24
  */

import com.typesafe.config.Config
import org.apache.spark.ml.common.AntiSpamModel
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._


trait FeaScoreTFParam extends Params {
  val expression = new Param[String](this,"expr","定义条件")
  val defaultscore = new Param[Double](this,"defaultscore","输入特征具体分数")
}
class FeaScoreTF(override val uid:String) extends AntiSpamModel[FeaScoreTF] with FeaScoreTFParam{
  override def setConfig(c: Config): this.type = {
    super.setConfig(c)
    set(expression->c.getString("expression"))
    set(defaultscore->c.getDouble("defaultscore"))
    this
  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputs = ${outputCols}(0)
    val score = ${defaultscore}
    val input = ${inputCols}(0)
    val feaData = dataset.withColumn(outputs,when(expr(${expression}),col(input)).otherwise(score))
    feaData
  }

  override def copy(extra: ParamMap): Nothing = ???
}
