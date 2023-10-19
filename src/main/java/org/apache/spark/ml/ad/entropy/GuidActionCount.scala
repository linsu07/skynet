package org.apache.spark.ml.ad.entropy

/**
  * @author qinsha
  * @create 2020/05/20
  *  功能： 渠道中用户行为个数的分布
  */

import com.typesafe.config.Config
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.algo.RelativeEntropy
import org.apache.spark.ml.common.{AntiSpamAlgorithm, AntiSpamModel, Utils}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
trait GuidActionCountParam extends Params{
  val maxAction = new IntParam(this,"maxAction","")
  setDefault(maxAction->20)
}

class GuidActionCount(override val uid:String) extends AntiSpamModel[GuidActionCount] with GuidActionCountParam{
  def this() = this(Identifiable.randomUID("GuidActionCount"))

  override def setConfig(c: Config): GuidActionCount.this.type = {
    if(c.hasPath("maxAction"))
      set(maxAction->c.getInt("maxAction"))
    super.setConfig(c)
  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    val statDims  = getStatDimension()
    val data = dataset.asInstanceOf[DataFrame]
    val countActions = udf{
      (actions:Seq[String])=>
        val len = {if(actions.length<=${maxAction}) actions.length.toString else ${maxAction}.toString}
        if(len>"0")
          s"${len}:1"
        else
          ""
    }
    data.withColumn(${outputCols}(0),countActions(col(${inputCols}(0))))
  }
  override def copy(extra: ParamMap): GuidActionCount = ???
}
