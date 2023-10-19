package org.apache.spark.ml.ad.features

import org.apache.spark.sql.functions._
import org.apache.spark.ml.common.AntiSpamModel
import org.apache.spark.ml.common.ua.UserAgent
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset}

//UserAgent userAgent = UserAgent.parseUserAgentString
class BrowserExtractModel (override val uid:String) extends AntiSpamModel{
  override def transform(dataset: Dataset[_]): DataFrame = {
    val extract = udf{
      (ua:String)=>
        val agent = UserAgent.parseUserAgentString(ua)
        agent.getBrowser.getName
    }
    val extract2 = udf{
      (ua:String)=>
        var agent = ua.split(" ").last
        agent.split("/")(0)
    }
    dataset.withColumn(${outputCols}(0),extract(col(${inputCols}(0))))
//    ret.withColumn("ua_last",extract2(col(${inputCols}(0))))
  }
  override def copy(extra: ParamMap): Nothing = ???
}
