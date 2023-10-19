package org.apache.spark.ml.ad.features

import com.typesafe.config.Config
import org.apache.spark.ml.common.AntiSpamModel
import org.apache.spark.ml.param.{ParamMap, Params, StringArrayParam}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._


import scala.collection.JavaConverters._
trait SrcExtractParam extends Params{
  val srcNameList = new StringArrayParam(this,"srcNameList","")
  val exprList = new StringArrayParam(this,"exprList","")
}

class SrcExtractModel (override val uid:String) extends AntiSpamModel with SrcExtractParam{
  override def setConfig(c: Config): this.type = {
    super.setConfig(c)
    val configList = c.getConfigList("src_list")
    set(srcNameList->configList.asScala.toArray.map(_.getString("name")))
    set(exprList->configList.asScala.toArray.map(_.getString("expr")))
    this
  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    val zipped=${exprList}.zip(${srcNameList})
    val extractSrc = udf{
      (src:String)=>
        val ret = zipped.find{case (expr,name) =>
          expr.r.findFirstIn(src)!=None
        }
        if(ret.nonEmpty)
          ret.get._2
        else
          "unk"
    }
    dataset.withColumn(${outputCols}(0),extractSrc(col(${inputCols}(0))))
  }

  override def copy(extra: ParamMap): Nothing = ???
}
