package net.qihoo.antispam.application.pc.report

import org.apache.spark.ml.common.{CheckStatus, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

class Statistics4Pc(val result:Dataset[Row],val nameArray:Array[String])extends Serializable {
  val resultFrame = result
  def combName(algoName:String,status:String): String ={
    "%s%s%s".format(algoName,Utils.sep,status)
  }
  var statDataset: Seq[(String,mutable.HashMap[String,Double])] = {
    resultFrame.rdd.aggregate(new mutable.HashMap[String,mutable.HashMap[String,Double]]())(
      (bmap:mutable.HashMap[String,mutable.HashMap[String,Double]],row:Row)=> {
        val site = row.getAs[String]("site")
        var chl = row.getAs[String](Statistics.groupName)
        var ctype = row.getAs[String]("ctype")
        var sadspace = row.getAs[String]("sadspace")
        if(sadspace==null)
          sadspace = "0"
        var n = combName(site,chl)
         n = combName(n,ctype)
        n = combName(n,"2")
        val name = combName(n,sadspace)
        val map = bmap.getOrElseUpdate(name,new mutable.HashMap[String,Double]())
        map.put(Statistics.searchNum, 1.0 + map.getOrElse(Statistics.searchNum, 0.0))
        val checkResult = nameArray.map { name =>
          (name, row.getAs[Int](name))
        }
        try {
          var finalStatus = math.max(checkResult.map(_._2).max, CheckStatus.real.id)
          merge2map(Statistics.finalStatus, finalStatus, map, checkResult)
          checkResult.foreach { case (name, id) =>
            merge2map(name, id, map, null)
          }
        } catch {
          case e: Throwable =>
            println("statDataset statistic err")
            e.printStackTrace()
        }
        bmap
      },
      (map1:mutable.HashMap[String,mutable.HashMap[String,Double]],map2:mutable.HashMap[String,mutable.HashMap[String,Double]])=>{
        for((name,innerMap)<-map2) {
          val inerMap1 = map1.getOrElseUpdate(name, new mutable.HashMap[String, Double]())
          for ((key, value)<- innerMap)
            inerMap1.put(key, value + inerMap1.getOrElse(key, 0.0))
        }
        map1
      }
    ).toSeq
  }
  def merge2map(algoName:String,id:Int,map:mutable.HashMap[String,Double],checkResult:Array[(String,Int)]): Unit ={
    val status = CheckStatus(id).toString
    val name = combName(algoName,status)
    map.put(name,1.0+map.getOrElse(name,0.0))
    if(algoName.equals(Statistics.finalStatus)){
      val num = checkResult.filter(_._2 >= CheckStatus.anomaly.id).length
      map.put(Statistics.avgCheckedout, num + map.getOrElse(Statistics.avgCheckedout, 0.0))
      if(id==CheckStatus.anomaly.id){
        if(checkResult.filter(_._2 == CheckStatus.anomaly.id).length>2)
          map.put(Statistics.mergedAnomaly,1+map.getOrElse(Statistics.mergedAnomaly, 0.0))
      }
    }
  }
  def generateCMSheet(spark:SparkSession): DataFrame ={
    import spark.implicits._
    val sheet = statDataset.map{ case(key,statMap)=>
      val searchNum = statMap.getOrElse(Statistics.searchNum,0.0)
      var spamNum = statMap.getOrElse(combName(Statistics.finalStatus,CheckStatus.spam.toString),0.0)
      val oriSpam = spamNum
      val anomalyNum = statMap.getOrElse(combName(Statistics.finalStatus,CheckStatus.anomaly.toString),0.0)
      val realNum = statMap.getOrElse(combName(Statistics.finalStatus,CheckStatus.real.toString),0.0)
      val avgCheckedout = if((anomalyNum+spamNum)==0) 0 else statMap.getOrElse(Statistics.avgCheckedout,0.0)/(anomalyNum+spamNum)
      val mergedAnom = statMap.getOrElse(Statistics.mergedAnomaly,0.0)
      if(mergedAnom>0.0)
        spamNum = spamNum+mergedAnom
      val valid = searchNum - spamNum
      val splits = key.split("#")
      val site = splits(0)
      val chl = splits(1)
      val ctype = splits(2)
      val jt = splits(3)
      val sadspace = splits(4)
      (site,chl,ctype,jt,sadspace,searchNum,(valid*0.95).round)  // 5% 的扣减在这里直接加上了
    }.toDF("site","channel","ctype","jt","sadspace",Statistics.searchNum,"valid")
    sheet
//    sheet.repartition(1).write.mode("overwrite").option("header","true").csv(path)
  }
}
