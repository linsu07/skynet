package net.qihoo.antispam.application.pc.report

import java.net.URL

import org.apache.spark.ml.common.{CheckStatus, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @author linsu at 2020/03/11 8:19
  * @param result
  */

class Statistics(val result:Dataset[Row],val nameArray:Array[String]) extends Serializable {
  val resultFrame = result
  val curSite = new mutable.HashSet[String]()
  curSite.add("www.wb123.com")
  curSite.add("hao.ylmf.com")
  curSite.add("www.jiegeng.com")
  curSite.add("www.newduba.cn")
  curSite.add("nav.bohaiqilin.com")
  curSite.add("hao.lenovo.com.cn")
  curSite.add("www.112112.com")
  curSite.add("www.160.com")
  curSite.add("115.com")
  curSite.add("www.52daohang.com")

  def combName(algoName:String,status:String): String ={
    "%s%s%s".format(algoName,Utils.sep,status)
  }
  var statDataset: Seq[(String,mutable.HashMap[String,Double])] = {
      resultFrame.rdd.aggregate(new mutable.HashMap[String, mutable.HashMap[String, Double]]())(
        (bmap: mutable.HashMap[String, mutable.HashMap[String, Double]], row: Row) => {
          var chl = row.getAs[String](Statistics.groupName)
          var site = row.getAs[String]("site")
          site = site.replace("https://","").replace("http://","")
          val name = combName(site,chl)
          val map = bmap.getOrElseUpdate(name, new mutable.HashMap[String, Double]())
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
        (map1: mutable.HashMap[String, mutable.HashMap[String, Double]], map2: mutable.HashMap[String, mutable.HashMap[String, Double]]) => {
          for ((name, innerMap) <- map2) {
            val inerMap1 = map1.getOrElseUpdate(name, new mutable.HashMap[String, Double]())
            for ((key, value) <- innerMap)
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
      var num = checkResult.filter(_._2 >= CheckStatus.anomaly.id).length
      if((num>2)||(id==CheckStatus.spam.id))
        map.put(Statistics.avgCheckedout, num + map.getOrElse(Statistics.avgCheckedout, 0.0))
      if(id==CheckStatus.anomaly.id){
        if(num>2)
          map.put(Statistics.mergedAnomaly,1+map.getOrElse(Statistics.mergedAnomaly, 0.0))
      }
    }
  }
  def generateCMSheet(spark:SparkSession,path:String): DataFrame ={
    import spark.implicits._
    val sheet = statDataset.map{ case(key,statMap)=>
      val searchNum = statMap.getOrElse(Statistics.searchNum,0.0)
      var spamNum = statMap.getOrElse(combName(Statistics.finalStatus,CheckStatus.spam.toString),0.0)
      val oriSpam = spamNum
      val anomalyNum = statMap.getOrElse(combName(Statistics.finalStatus,CheckStatus.anomaly.toString),0.0)
      val realNum = statMap.getOrElse(combName(Statistics.finalStatus,CheckStatus.real.toString),0.0)

      val mergedAnom = statMap.getOrElse(Statistics.mergedAnomaly,0.0)
      if(mergedAnom>0.0)
        spamNum = spamNum+mergedAnom
      val avgCheckedout = if(spamNum==0) 0 else statMap.getOrElse(Statistics.avgCheckedout,0.0)/(spamNum)

      val valid = searchNum - spamNum
      val splits = key.split(Utils.sep)
      val site = splits(0)
      val chl = splits(1)
      (site,chl,searchNum,realNum,anomalyNum,oriSpam,mergedAnom,valid,spamNum,avgCheckedout)
    }.toDF("site","channel",Statistics.searchNum,CheckStatus.real.toString,CheckStatus.anomaly.toString
      ,"oriSpam",Statistics.mergedAnomaly,"valid",CheckStatus.spam.toString,Statistics.avgCheckedout)

//            (chl,searchNum,spamNum)
//          }.toDF("channel",Statistics.searchNum,CheckStatus.spam.toString)

      sheet.repartition(1)
  }
  import scala.collection.convert._

  def generateAlgoStatisticsByChl(spark:SparkSession,path:String): Unit ={
//    val colName = new ListBuffer[StructField]()
//    colName.appendAll(Array("channel","algorithm").map(new StructField(_,StringType,false)))
//    colName.appendAll(CheckStatus.values.toArray.map(_.toString).map(new StructField(_,DoubleType,false)))
//    val schema = new StructType(colName.toArray)
    val fields = Array("channel","algorithm").map(new StructField(_,StringType,nullable = false))
      .union(CheckStatus.values.toArray.map(_.toString).map(new StructField(_,DoubleType,false)))
    val schema = new StructType(fields)

    var ret = interGenerate(spark,path)
    val rdd = ret.map{case (chl,name,statusNumbers)=>
      val rowArray = new mutable.ListBuffer[Any]()
      rowArray.append(chl)
      rowArray.append(name)
      rowArray.appendAll(statusNumbers)

      new GenericRow(rowArray.toArray).asInstanceOf[Row]
    }
    val retFrame = spark.createDataFrame(spark.sparkContext.parallelize(rdd).repartition(1),schema)
    retFrame.repartition(1).write.mode("overwrite").option("header","true").csv(path)
  }
  private def interGenerate(spark:SparkSession,path:String)={
    val ret = statDataset.flatMap { case (chl, statMap) =>
      nameArray.map{ name=>
        val statusNumbers = CheckStatus.values.toArray.map{ value=>
          val comName = combName(name,value.toString)
          statMap.getOrElse(comName,0.0)
        }
        (chl,name,statusNumbers)
      }
    }
    ret
  }
  def generateAlgoStatistics(spark:SparkSession,path:String): Unit ={
    val colName = new ListBuffer[StructField]()
    colName.appendAll(Array("algorithm").map(new StructField(_,StringType,false)))
    colName.appendAll(CheckStatus.values.toArray.map(_.toString).map(new StructField(_,DoubleType,false)))
    val schema = new StructType(colName.toArray)
    var ret = spark.sparkContext.parallelize(interGenerate(spark,path)).repartition(1)
    val rdd = ret.map{case (chl,name,statusNumbers)=>
      (name,statusNumbers)
    }.reduceByKey{ (numbers1,numbers2)=>
//      val results = new ListBuffer[Double]()
//      for((n1,n2)<-numbers1.zip(numbers2))
//        results.append(n1+n2)
//      results.toArray
      numbers1.zip(numbers2).map{case(n1,n2)=>n1+n2}.toArray
    }.map{case (name,statusNumbers)=>
      val rowArray = new mutable.ListBuffer[Any]()
      rowArray.append(name)
      rowArray.appendAll(statusNumbers)
      new GenericRow(rowArray.toArray).asInstanceOf[Row]
//      Row(name,statusNumbers:_*)
    }
    val retFrame = spark.createDataFrame(rdd,schema)
    retFrame.show(20,30)
    retFrame.repartition(1).write.mode("overwrite").option("header","true").csv(path)
  }

  val correlationNum = 10
  def generateCorrelation(spark:SparkSession,pathBySrcg:String,path:String):Unit = {
    val corSep = "&&&"
    val correlationWithSrcg = resultFrame.rdd.map { row =>
      val chl = row.getAs[String]("srcg")
      val workedRules = nameArray.map { name =>
        (name, row.getAs[Int](name))
      }.filter(_._2 >= CheckStatus.anomaly.id).map(_._1).sorted
      val totalMap = new mutable.HashMap[String, Int]()
      val independentMap = new mutable.HashMap[String, Int]()
      val correlationMap = new mutable.HashMap[String, Int]()
      totalMap ++= workedRules.map((_, 1)).toMap
      if (workedRules.length > 0) {
        if (workedRules.length == 1)
          independentMap.put(workedRules(0), 1)
        else if(workedRules.length==2){
          val short = workedRules(0).split("By")(0)
          val short2 = workedRules(1).split("By")(0)
          if(short.equals(short2))
            independentMap.put(workedRules(0), 1)
          else{
            correlationMap.put(workedRules.mkString(corSep), 1)
          }
        }
        else {
          for (comp <- workedRules.combinations(2)){
            val short = comp(0).split("By")(0)
            val short2 = comp(1).split("By")(0)
            if(!short.equals(short2))
              correlationMap.put(comp.mkString(corSep), 1)
          }
        }
      }
      (chl, (totalMap, independentMap, correlationMap))
    }.reduceByKey { case ((tmap1, imap1, cmap1), (tmap2, imap2, cmap2)) =>
      for ((name, value) <- tmap2)
        tmap1.put(name, value + tmap1.getOrElse(name, 0))
      for ((name, value) <- imap2)
        imap1.put(name, value + imap1.getOrElse(name, 0))
      for ((name, value) <- cmap2)
        cmap1.put(name, value + cmap1.getOrElse(name, 0))
      (tmap1, imap1, cmap1)
    }.cache()
    val corBysrcgRdd  = correlationWithSrcg.flatMap { case (chl, maps) =>
      val totalMap = maps._1
      val independentMap = maps._2
      val correlationList = maps._3.toArray

      val rows = new ListBuffer[Row]()
      for ((name, count) <- totalMap.toArray) {
        val corrList = correlationList.map { case (corr: String, joined: Int) =>
          val names = corr.split(corSep, -1)
          (names, joined)
        }.filter(_._1.find(_.equals(name)) != None).map { case (corr, joined) =>
          val couterpart = corr.filter(!_.equals(name))(0)
          (couterpart, joined)
        }.sortBy(_._2).reverse
        val len = if (corrList.length > correlationNum) correlationNum else corrList.length
        for (i <- 0 until (len)) {
          val corName = corrList(i)._1
          val joined = corrList(i)._2
          val corCount = totalMap.getOrElse(corName, 1)
          val row = Row(chl, name, count, independentMap.getOrElse(name, 0), corName, corCount, joined, joined.toDouble / (count + corCount - joined).toDouble)
          rows.append(row)
        }
      }
      rows
    }

    val corBysrcgFrame = spark.createDataFrame(corBysrcgRdd, new StructType()
      .add(new StructField("srcg", StringType, false))
      .add(new StructField("algo-name", StringType, false))
      .add(new StructField("algo-checked", IntegerType, false))
      .add(new StructField("independent-checked", IntegerType, false))
      .add(new StructField("correlated-algo-name", StringType, false))
      .add(new StructField("correlated-algo-checked", IntegerType, false))
      .add(new StructField("innerjoined", IntegerType, false))
      .add(new StructField("Jaccard coefficient", DoubleType, false))
    )
//    corBysrcgFrame.show(30, 100)
    corBysrcgFrame.repartition(1).write.mode("overwrite").option("header", "true").csv(pathBySrcg)

    val maps = correlationWithSrcg.map(_._2).reduce { case ((tmap1, imap1, cmap1), (tmap2, imap2, cmap2)) =>
      for ((name, value) <- tmap2)
        tmap1.put(name, value + tmap1.getOrElse(name, 0))
      for ((name, value) <- imap2)
        imap1.put(name, value + imap1.getOrElse(name, 0))
      for ((name, value) <- cmap2)
        cmap1.put(name, value + cmap1.getOrElse(name, 0))
      (tmap1, imap1, cmap1)
    }
    val totalMap = maps._1
    val independentMap = maps._2
    val correlationList = maps._3
    val rows = new ListBuffer[Row]()

    for ((name, count) <- totalMap.toArray) {
      val corrList = correlationList.toArray.map { case (corr: String, joined: Int) =>
        val names = corr.split(corSep, -1)
        (names, joined)
      }.filter(_._1.find(_.equals(name)) != None).map { case (corr, joined) =>
        val couterpart = corr.filter(!_.equals(name))(0)
        (couterpart, joined)
      }.sortBy(_._2).reverse
      val len = if (corrList.length > correlationNum) correlationNum else corrList.length
      for (i <- 0 until (len)) {
        val corName = corrList(i)._1
        val joined = corrList(i)._2
        val corCount = totalMap.getOrElse(corName, 1)
        val row = Row(name, count, independentMap.getOrElse(name, 0), corName, corCount, joined, joined.toDouble / (count + corCount - joined).toDouble)
        rows.append(row)
      }
    }
    val rowRdd = spark.sparkContext.parallelize(rows)
    val corFrame = spark.createDataFrame(rowRdd, new StructType()
      .add(new StructField("algo-name", StringType, false))
      .add(new StructField("algo-checked", IntegerType, false))
      .add(new StructField("independent-checked", IntegerType, false))
      .add(new StructField("correlated-algo-name", StringType, false))
      .add(new StructField("correlated-algo-checked", IntegerType, false))
      .add(new StructField("innerjoined", IntegerType, false))
      .add(new StructField("Jaccard coefficient", DoubleType, false))
    )
    corFrame.show(30, 100)
    corFrame.repartition(1).write.mode("overwrite").option("header", "true").csv(path)
  }
}

object Statistics{
  val finalStatus = "finalStatus"
  val finalScore = "QualityScore"
  val groupName = "srcg"
//  val spamNum ="AvgSpamNum"
  val avgCheckedout= "AvgCheckoutNum"
  val searchNum = "searchNum"
  val mergedAnomaly = "mergedAnomaly"
  val blackGroupName = "guid"
}
