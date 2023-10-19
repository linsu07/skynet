package org.apache.spark.ml.ad.features

import com.google.gson.JsonParser
import com.typesafe.config.Config
import org.apache.spark.ml.common.AntiSpamModel
import org.apache.spark.ml.param.{ParamMap, Params, StringArrayParam}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.xerces.xs.StringList

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class SearchEntranceTypeTF(override val uid:String) extends AntiSpamModel {

  val search1list = new ListBuffer[String]()
  override def setConfig(c: Config): this.type = {
    super.setConfig(c)
    search1list.appendAll(c.getStringList("search1").asScala)
    this
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val addSearchType = udf{
      (src: String)=>
        val findopt = search1list.find{ case (exr) =>
          exr.r.findFirstIn(src)!=None
        }
        if(findopt==None)
          s"search2"
        else
          s"search1"

    }
    var searchData = dataset.withColumn(${outputCols}(0),addSearchType(col(${inputCols}(0))))
    searchData
  }
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): Nothing = ???
}
