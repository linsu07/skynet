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


trait FeaBasicOperationTFParam extends Params {
  val expression = new Param[String](this,"expr","定义条件")
}
class FeaBasicOperationTF(override val uid:String) extends AntiSpamModel[FeaBasicOperationTF] with FeaBasicOperationTFParam{
  override def setConfig(c: Config): this.type = {
    super.setConfig(c)
    set(expression->c.getString("expression"))
    this
  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputs = ${outputCols}(0)
    val feaData = dataset.withColumn(outputs,expr(${expression}))
    feaData
  }

  override def copy(extra: ParamMap): Nothing = ???
}
