package org.apache.spark.ml.ad.entropy

import com.typesafe.config.Config
import org.apache.spark.ml.common.AntiSpamModel
import org.apache.spark.ml.param.{DoubleParam, IntParam, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait GuidTimeGapParam extends Params{
  val timeStepMilliSecond = new DoubleParam(this,"timeStepMilliSecond","",ParamValidators.gt(0))
  val maxStep = new IntParam(this,"maxStep","",ParamValidators.gt(0))
  val sessionGap = new IntParam(this,"sessionGap","",ParamValidators.gt(0))
}

class GuidTimeGap(override val uid:String) extends AntiSpamModel[GuidTimeGap] with GuidTimeGapParam{
  def this() = this(Identifiable.randomUID("GuidTimeGap"))

  override def setConfig(c: Config): GuidTimeGap.this.type = {
    set(timeStepMilliSecond->c.getDouble("timeStepMilliSecond"))
    set(maxStep->c.getInt("maxStep"))
    set(sessionGap->c.getInt("sessionGap"))
    super.setConfig(c)
  }
  override def transform(dataset: Dataset[_]): DataFrame = {
    val statDims  = getStatDimension()
    val data = dataset.asInstanceOf[DataFrame]
    val countActions = udf{
      (time:Seq[String])=>
        val len = time.size
        val buf = new mutable.HashMap[String,Int]()
        if(len>1) {
          for (i <- 0.until(len - 1)) {
            val gap_temp = ((time(i + 1).toLong-time(i).toLong)/${timeStepMilliSecond}).floor
            if (gap_temp <= ${sessionGap}) {
              val gap = if (gap_temp > ${maxStep} - 1) ${maxStep} - 1 else gap_temp
              buf.put(gap.toInt.toString,1+buf.getOrElse(gap.toInt.toString,0))
            }
          }
          val ret = buf.map{case(key,value)=>s"${key}:${value}"}.mkString("|")
          ret
        }else
          ""
    }
    data.withColumn(${outputCols}(0),countActions(col(${inputCols}(0))))
  }
  override def copy(extra: ParamMap): GuidTimeGap = ???
}