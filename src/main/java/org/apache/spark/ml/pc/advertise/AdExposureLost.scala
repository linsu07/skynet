package org.apache.spark.ml.pc.advertise

/**
  * @author linsu
  * @Date 2020/3/17
结果页面的top有广告，但是没有发现相应的广告曝光
tg字段有top的广告，但是ads_details 里面却没有相应的曝光
  */
import org.apache.spark.ml.common.{AntiSpamAlgorithm, AntiSpamModel, CheckStatus, Utils}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.pc.AdsHelper
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable}

class AdExposureLost(override val uid:String)
  extends AntiSpamModel[AdExposureLost]  with AntiSpamAlgorithm with DefaultParamsWritable{
  override def copy(extra: ParamMap): AdExposureLost = {
    copyValues(new AdExposureLost(uid).setParent(parent), extra)
  }
  override def check(keys: Seq[Any], inputs: Seq[Any]): Int = {
    val key= keys.mkString(Utils.sep)
    //top|keyword|1|182,bottom|keyword|1,right|keyword|1
    val tg = inputs(0).toString
    if (tg.isEmpty)
      return CheckStatus.real.id
    val isTopAd = tg.split(",").map{ s=>
      val strs= s.split("\\|")
      if(strs.length>1)
        strs(0)
      else
        "None"
//    }.find(_.equals("top"))!=None
    }.find(_.equals("top"))!=None
    if(!isTopAd)
      return CheckStatus.real.id
    val ad = inputs(1)
    val adshow = AdsHelper.getFromString(inputs(1).toString)

    val existTopAdExposure = adshow.map(_.pos).find(_.equals("1"))!=None
    if(existTopAdExposure) {
      CheckStatus.real.id
    } else
      CheckStatus.spam.id
  }
}
object AdExposureLost extends DefaultParamsReadable[AdExposureLost]