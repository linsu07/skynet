package org.apache.spark.ml.pc.advertise

/**
  * @author qinsha
  * @create 2020/03/16
  *  功能： 查看广告反作弊判别情况，只判为疑似
  */

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.common.{AntiSpamModel, CheckStatus, ModelPath, Utils}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.pc.AdclickHelper
import org.apache.spark.ml.util._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}



//suv, spv,cuvOuter,cpvOuter,cuvInner,cpvInner
class AdAntispamgroupJudgeModel(override val uid: String,private var AdAntispamgroupJudge:  Array[(String,Double,Double,Double)])
  extends AntiSpamModel[AdAntispamgroupJudgeModel]  with MLWritable{

  var anomalyroup:Set[String] = _  //AdAntispamgroupJudge.map(_._1).toSet
  var boastcast:Broadcast[Set[String]] = _

  def setanomalyMap(spamgroup:Set[String])={
    this.anomalyroup = spamgroup
    this
  }
  override def copy(extra: ParamMap): AdAntispamgroupJudgeModel = {
    copyValues(new AdAntispamgroupJudgeModel(uid,AdAntispamgroupJudge).setParent(parent),extra)
  }
  override def write: MLWriter = new AdAntispamgroupJudgeModel.ModelWriter(this)

  override def addBroadcast(spark:SparkSession)={
    boastcast = spark.sparkContext.broadcast(anomalyroup)
  }
  override def check(key: Seq[Any], inputs: Seq[Any]): Int = {
    val groupKey = key.mkString(Utils.sep)
    val positions = {
      val adclick = AdclickHelper.getFromString(inputs(1).toString)
      adclick.map { click =>
        click.spam
      }
    }
    if (boastcast.value.contains(groupKey) && positions.contains("1"))
    {CheckStatus.anomaly.id}
    else
    {CheckStatus.real.id}
  }

}

object AdAntispamgroupJudgeModel extends MLReadable[AdAntispamgroupJudgeModel] with ModelPath {
  private[AdAntispamgroupJudgeModel] case class groupIp(groupKey:String, ip:String)
  private[AdAntispamgroupJudgeModel] class ModelWriter(instance: AdAntispamgroupJudgeModel) extends MLWriter {
    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val AdAntispamgroupJudge = instance.AdAntispamgroupJudge.toSeq
      val dataPath = getStatPath(path)
      sparkSession.createDataFrame(AdAntispamgroupJudge).toDF(instance.getStatDimension().mkString(Utils.sep),"adjudgetotal","adjudgespam", "judgerate")
        .repartition(1).write.mode(SaveMode.Overwrite).option("header","true").csv(dataPath)
    }

  }

  override def read: MLReader[AdAntispamgroupJudgeModel] = new AdAntispamgroupJudgeModel.ModelReader

  private class ModelReader extends MLReader[AdAntispamgroupJudgeModel] {
    /** Checked against metadata when loading model */
    private val className = classOf[AdAntispamgroupJudgeModel].getName

    override def load(path: String): AdAntispamgroupJudgeModel = {
      val model = new DefaultParamsReader[AdAntispamgroupJudgeModel].load(path) // model 必须实现 this（uid:String）方法
      val dataPath = getStatPath(path)
      val spamMap = sparkSession.read.option("header", "true").csv(dataPath).rdd.map { row =>
        row.getString(0)
      }.collect().toSet
      model.anomalyroup = spamMap
      model
    }
  }
}
