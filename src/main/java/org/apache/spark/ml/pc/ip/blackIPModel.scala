package org.apache.spark.ml.pc.ip

import org.apache.spark.ml.common.{AntiSpamAlgorithm, AntiSpamModel, CheckStatus, ModelPath, Utils}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsReader, DefaultParamsWritable, DefaultParamsWriter, MLReadable, MLReader, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.functions.{col, udf}

class BlackIPModel(override val uid:String,var blackips:Set[String])
  extends AntiSpamModel[BlackIPModel] with AntiSpamAlgorithm with DefaultParamsWritable{
  def this(uid:String) = this(uid,null)
  override def copy(extra: ParamMap): BlackIPModel = {
    copyValues(new BlackIPModel(uid, blackips).setParent(parent), extra)
  }
  override def write: MLWriter = new BlackIPModel.ModelWriter(this)
  override def transform(data:Dataset[_]): DataFrame = {
    val broadcast = data.sparkSession.sparkContext.broadcast(blackips)
    val ipCheck = udf{
      (srcg:String,ip:String)=>
        if(broadcast.value.contains(Array(srcg,ip).mkString(Utils.sep)))
          CheckStatus.spam.id
        else
          CheckStatus.real.id
    }
    data.withColumn(getName(),ipCheck(col(${inputCols}(0)),col(${inputCols}(1))))
  }
}

object BlackIPModel extends MLReadable[BlackIPModel] with ModelPath {
  private[BlackIPModel] class ModelWriter(instance: BlackIPModel) extends MLWriter {
    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = getStatPath(path)
      sparkSession.createDataFrame(instance.blackips.map((_,1)).toSeq).toDF("ip","nothing")
        .repartition(1).write.mode(SaveMode.Overwrite).option("header","true").csv(dataPath)
    }

  }

  override def read: MLReader[BlackIPModel] = new BlackIPModel.ModelReader

  private class ModelReader extends MLReader[BlackIPModel] {
    /** Checked against metadata when loading model */
    private val className = classOf[BlackIPModel].getName

    override def load(path: String): BlackIPModel = {
      val model = new DefaultParamsReader[BlackIPModel].load(path) // model 必须实现 this（uid:String）方法
      val dataPath = getStatPath(path)
      val spamMap = sparkSession.read.option("header", "true").csv(dataPath).rdd.map { row =>
        row.getString(0)
      }.collect().toSet
      model.blackips = spamMap
      model
    }
  }
}