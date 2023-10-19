package net.qihoo.antispam.application.common

case class Param(product:String,day:String,searchRoot:String,outputRoot:String,conf:String,dayWindow:String,allUserDataPath:String) extends Serializable{
  val searchPath = PathHelper.mkPath(Array(searchRoot,day))
  val CMSheetPath = PathHelper.mkPath(Array(outputRoot,"cm.sheets_temp",day))
  val MidFeaturePath = PathHelper.mkPath(Array(outputRoot,"features",day))
  val RetBlackPath = PathHelper.mkPath(Array(outputRoot,"ret.black",day))
  val SettlementPath = PathHelper.mkPath(Array(outputRoot,"cm.settlement.sheets",day))
//  val SettlementStatPath = PathHelper.mkPath(Array(outputRoot,product,day,"settlement.stat"))
  val RulesStatistisPath = PathHelper.mkPath(Array(outputRoot,"rules.statistics",day))
  val RulesChannelStatisticsPath = PathHelper.mkPath(Array(outputRoot,"rules.chl.statistics",day))
  val RulesChannelAgeStatisticsPath = PathHelper.mkPath(Array(outputRoot,"rules.chlage.statistics",day))
  val ReportFakeLsPath = PathHelper.mkPath(Array(outputRoot,"report.fakels",day))
  val ReportActivePath = PathHelper.mkPath(Array(outputRoot,"report.active",day))
  val ReportIpUvPath = PathHelper.mkPath(Array(outputRoot,"report.ipuv",day))
  val ReportActiveUVPath = PathHelper.mkPath(Array(outputRoot,"report.activeuv",day))
//  val RulesDetailPath = PathHelper.mkPath(Array(outputRoot,product,day,"rules.detail"))
  val CorrelationPath = PathHelper.mkPath(Array(outputRoot,"rules.correlation",day))
  val CorrelationChlPath = PathHelper.mkPath(Array(outputRoot,"rules.chl.correlation",day))
//  val RulesParquetReusltPath = PathHelper.mkPath(Array(outputRoot,product,day,"rules.parquet.results"))
  val modelPath = PathHelper.mkPath(Array(outputRoot,"model",day))
  val modelParquetPath = PathHelper.mkPath(Array(outputRoot,"model.parquet",day))
  val qualityPath = PathHelper.mkPath(Array(outputRoot,"cm.quality.sheets",day))
  val guidblackLibrayPath = PathHelper.mkPath(Array(outputRoot,"guid.blacklibrary",day))
  val cmMiddlePath = PathHelper.getPath(outputRoot,"cm.middle.sheets",day)
  val starLevelStatisticsPath =   PathHelper.mkPath(Array(outputRoot,"starLevel.statistics",day))
  val starLevelPath = PathHelper.mkPath(Array(outputRoot,"cm.starLevel_temp",day))
  val reStarPath = PathHelper.getPath(outputRoot,"cm.reStarPath",day)
  val ReportCostPath = PathHelper.mkPath(Array(outputRoot,"report.cost",day))

}
object ParamFactory{
  def getParams(args:Array[String]):Param = {
    val args_map = Utils.parseArgs(args)
    //day=20170405  inputPath=sample/device_info product=weishi outputPath=d:\tmp\cmReport
    println(args_map)
    val p = new Param(args_map.get("product").get
      ,args_map.get("day").get,args_map.get("searchPath").get,args_map.get("outputPath").get
      ,if(args_map.get("conf")!=None) args_map.get("conf").get else null,
      if(args_map.get("dayWindow")!=None) args_map.get("dayWindow").get else "1",
      if(args_map.get("allUserDataPath")!=None) args_map.get("allUserDataPath").get else null
      )
    p
  }
}
