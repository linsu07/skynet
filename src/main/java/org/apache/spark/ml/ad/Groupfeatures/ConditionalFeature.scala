package org.apache.spark.ml.ad.Groupfeatures

import com.typesafe.config.Config
import org.apache.spark.ml.common.{AntiSpamModel, CheckStatus}
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, Params}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, expr, udf, when}

trait ConditionalFeatureParam extends Params {
    val ifExpr = new Param[String](this,"ifExpr","定义条件1")
    val elseifExpr = new Param[String](this,"elseifExpr","定义条件2")
    val ifValueExpr = new Param[String](this,"ifValueExpr","定义条件1结果")
    val elseifValueExpr = new Param[String](this,"elseifValueExpr","定义条件2结果")
    val defaultValue = new Param[String](this,"defaultValue","缺省结果")
}
  class ConditionalFeature(override val uid:String) extends AntiSpamModel[ConditionalFeature] with ConditionalFeatureParam{
    setDefault(ifExpr->"",elseifExpr->"",ifValueExpr->"",elseifExpr->"",defaultValue->"")
    override def setConfig(c: Config): this.type = {
      super.setConfig(c)
      if(c.hasPath("ifExpr"))
        set(ifExpr->c.getString("ifExpr"))
      if(c.hasPath("elseifExpr"))
        set(elseifExpr->c.getString("elseifExpr"))
      if(c.hasPath("ifValueExpr"))
        set(ifValueExpr->c.getString("ifValueExpr"))
      if(c.hasPath("elseifValueExpr"))
        set(elseifValueExpr->c.getString("elseifValueExpr"))
      if(c.hasPath("defaultValue"))
        set(defaultValue->c.getString("defaultValue"))
      this
    }
    override def transform(dataset: Dataset[_]): DataFrame = {
      val outputs = ${outputCols}(0)
      if((${ifExpr}.length>0)&&(${elseifExpr}.length>0))
        dataset.withColumn(${outputCols}(0),when(expr(${ifExpr}),expr(${ifValueExpr}))
          .when(expr(${elseifExpr}),expr(${elseifValueExpr}))
          .otherwise(expr(${defaultValue})))
      else if(${ifExpr}.length>0)
        dataset.withColumn(${outputCols}(0),when(expr(${ifExpr}),expr(${ifValueExpr}))
          .otherwise(expr(${defaultValue})))
      else
        throw new RuntimeException("ifExpr, elseifExpr must be set, pls check")
    }
    override def copy(extra: ParamMap): Nothing = ???
  }

