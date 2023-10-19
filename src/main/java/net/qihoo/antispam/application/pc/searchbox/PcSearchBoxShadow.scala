package net.qihoo.antispam.application.pc.searchbox

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.util.concurrent.{ExecutorService, Executors, ThreadPoolExecutor, TimeUnit}

import com.typesafe.config.{Config, ConfigFactory}
import net.qihoo.antispam.application.common.{GeneralApp, ParamFactory, PathHelper}
import net.qihoo.antispam.application.pc.report.{Statistics, Statistics4Pc}
import net.qihoo.antispam.application.pc.report
import net.qihoo.antispam.application.pc.searchbox.PcSearchBox.{config, param}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

import org.apache.spark.ml.common.{AggregateByKey, AntiSpamAlgorithm, CheckStatus, Context, HdfsHelper, Utils}
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
  * @author linsu at 2020/03/11 8:19
  */
trait PcSearchBoxShadow extends GeneralApp {
  val executor = Executors.newFixedThreadPool(10)
  val interModels = new mutable.HashMap[String,ListBuffer[Model[_]]]()
  def init(args: Array[String], configFile: String = null) = {
    param = ParamFactory.getParams(args)
    if (configFile != null)
      config = ConfigFactory.load(configFile)
    context = new Context().setCurDay(param.day).setProduct(param.product).setModelPath(param.modelPath)
  }

  def transformModel(model: Model[_], data: DataFrame,saved:Boolean = false): DataFrame = {
    executor.submit(new Runnable {
      override def run(): Unit = {
        val am= model.asInstanceOf[AntiSpamAlgorithm]
        val wm = model.asInstanceOf[MLWritable]
        try {
          wm.write.overwrite().save(PathHelper.mkPath(Array(context.modelPath, am.get(am.name).get)))
        }catch{
          case e:Throwable=>
            e.printStackTrace()
        }
      }
    })
    model.transform(data)
  }

  def startAntiSpam(spark: SparkSession, searchData: DataFrame, groupListName: String): (DataFrame, ListBuffer[Model[_]]) = {
    val modelList = new ListBuffer[Model[_]]()
    val groupList = config.getStringList(groupListName).asScala
    var ret = searchData
    for (groupName <- groupList) {
      config.checkValid(config, groupName)
      val gConfig = config.getConfig(groupName)
      println("in group %s . . .".format(groupName))
      val start = System.currentTimeMillis()
      val (curList, frame) = getModels(groupName, spark, gConfig, ret)
      println(s"${groupName} total time is ${(System.currentTimeMillis() - start) / 1000} seconds")
      ret = frame
      modelList.appendAll(curList)
    }

    (ret, modelList)
  }

  def getModels(groupName: String, spark: SparkSession, groupConfig: Config, data: DataFrame): (Array[Model[_]], DataFrame) = {
    val groupkeyConfig = if (groupConfig.hasPath("groupkey")) groupConfig.getConfig("groupkey") else null
    val dims = {
      val dimList = new ListBuffer[String]()
      if (groupkeyConfig != null && groupkeyConfig.hasPath("statDimension"))
        dimList ++= groupkeyConfig.getStringList("statDimension").asScala
      dimList
    }
    println("dims is ",dims)
    var algoList = {
      val ret = new ListBuffer[AntiSpamAlgorithm]()
      val list = groupConfig.getConfigList("algorithm_list").asScala.map { config =>
        val mergedConfig = {
          if (groupkeyConfig != null)
            config.withFallback(groupkeyConfig)
          else
            config
        }
        mergedConfig.checkValid(mergedConfig, "class")
        val algo = net.qihoo.antispam.application.common.Utils.getInstance(mergedConfig).asInstanceOf[AntiSpamAlgorithm]
        algo.setConfig(mergedConfig)
        algo.setContext(context)
        algo
      }
      ret.appendAll(list)
      ret
    }
    import spark.implicits._
    val middleSet = {
      if (groupkeyConfig == null)
        null
      else
        data.rdd.keyBy { row =>
          AntiSpamAlgorithm.getStatKey(dims.toArray, row)
        }.aggregateByKey(new mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]]())(
          (statMap: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]], row: Row) => {
            try {
              // 去掉同类型的算法，不需要那么多的aggregate
              algoList
                .filter(_.isInstanceOf[AggregateByKey]).foreach(_.asInstanceOf[AggregateByKey].seqOp(statMap, row))
            } catch {
              case e: Throwable =>
                e.printStackTrace()
            }
            statMap
          },
          (map1: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]], map2: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]]) => {
            try {
              algoList //.map{algo=>(algo.getClass.getSimpleName,algo)}.toMap.map(_._2)
                .filter(_.isInstanceOf[AggregateByKey]).foreach(_.asInstanceOf[AggregateByKey].combOp(map1, map2))
            } catch {
              case e: Throwable =>
                e.printStackTrace()
            }
            map1
          }
        )
    }
    val groupDataSet = if (middleSet == null) null else middleSet.toDS.cache()
    var groupframe = if (middleSet == null) null else {
      val tmpList = new ListBuffer[String]()
      tmpList.appendAll(dims)
      tmpList.append(AntiSpamAlgorithm.statColName)
      if (dims.length == 1) {
        middleSet.map { case (key, map) =>
          (key, map)
        }.toDF(tmpList: _*)
      } else if (dims.length == 2) {
        middleSet.map { case (key, map) =>
          val splits = key.split(Utils.sep, -1)
          (splits(0), splits(1), map)
        }.toDF(tmpList: _*)
      }
      else if (dims.length == 3) {
        middleSet.map { case (key, map) =>
          val splits = key.split(Utils.sep, -1)
          (splits(0), splits(1), splits(2), map)
        }.toDF(tmpList: _*)
      }
      else {
        middleSet.map { case (key, map) =>
          (key, map)
        }.toDF(tmpList: _*)
      }
    }

    val modelList = new ListBuffer[Model[_]]()
    val judgeModelOutputList = new ListBuffer[String]
    var retFrame = data
    var start = System.currentTimeMillis()
    if(interModels.get(groupName).nonEmpty){
      interModels.get(groupName).get.foreach(model=>
        groupframe = model.transform(groupframe))
    }

    val ret = algoList.foreach { algo =>
      //      println("making %s's model . . .".format(algo.getName()))
      start = System.currentTimeMillis()
      var model:Model[_] = null
      if(algo.isInstanceOf[Estimator[_]]){
        val estimator = algo.asInstanceOf[Estimator[_]]
        model = {
          if (algo.getTrainSource().equals("local"))
            estimator.fit(groupframe)
          else
            estimator.fit(groupDataSet)
        }.asInstanceOf[Model[_]]
      }else{
        model = algo.asInstanceOf[Model[_]]
      }
      if(algo.isaJudgeModel())
      {
        modelList.append(model)
        if (algo.getTransformTarget().contains("local"))
          judgeModelOutputList.appendAll(algo.getOutputCols)
      }

      var modelSaved = false
      if(algo.getTransformTarget().contains("local")) {
        groupframe = transformModel(model, groupframe, modelSaved)
        modelSaved = true
      }
      if(algo.getTransformTarget().contains("global")){
        retFrame = transformModel(model, retFrame,modelSaved)
        modelSaved = true
      }

      val other = algo.getTransformTarget().filter{ target=>
        (target.equals("local")==false)&&(target.equals("global")==false)
      }
      other.foreach(otherName=>
        interModels.getOrElseUpdate(otherName,new ListBuffer[Model[_]]).append(model)
      )
      println(s"${algo.getName()} running time is ${(System.currentTimeMillis() - start) / 1000} seconds")
    }

    // 各组特征拼接到global
    var majorcols = new ListBuffer[String]()
    if (groupConfig != null && groupConfig.hasPath("major_feature_outputcols")){
      if (groupConfig.hasPath("groupkey")){
        majorcols.appendAll(dims)
      }
      majorcols ++= groupConfig.getStringList("major_feature_outputcols").asScala
      val majorDf = groupframe.select(majorcols.map(k=> col(k)):_*)
      retFrame = retFrame.join(majorDf, dims, "left")
    }

    if (middleSet != null) {
      groupframe = groupframe.cache()
      handleGroupFeature(groupName, groupframe,judgeModelOutputList.toArray)
      groupDataSet.unpersist()
    }
    if(dims.nonEmpty){
      val needCols = new ListBuffer[String]
      needCols.appendAll(dims)
      needCols.appendAll(judgeModelOutputList)
      val newJudgeRes = groupframe.select(needCols.map(k=> col(k)):_*)
      retFrame = retFrame.join(newJudgeRes,dims.toSeq)
    }
    (modelList.toArray, retFrame)
  }
  def handleGroupFeature(groupName: String, groupframe: DataFrame, judgeCols:Array[String]): Unit = {}

  override def start(spark:SparkSession,data:DataFrame): DataFrame = {
    println("all the data coming")
    var searchData = data.filter(_.getAs[String]("tp")<="1")
    val begin = System.currentTimeMillis()
    //    searchData.show(20)
    var (ret_tmp,modelList) = startAntiSpam(spark,searchData,"rules_group")
    var ret = ret_tmp.coalesce(50).cache()
    println("check result is ...")
    //    ret.show(20,50)
    val models  = modelList.toArray.map(_.asInstanceOf[AntiSpamAlgorithm]).filter(_.isaJudgeModel())
    val nameList = new ListBuffer[String]()
    nameList.append("srcg","ctype","sadspace")
    nameList.appendAll(models.map(_.getName()))
    val r = ret.select("site",nameList:_*).cache()
    var maxSheets = new Statistics4Pc(r,models.map(_.getName())).generateCMSheet(spark)  // pc特有格式的输出
    println(models.map(_.getName()).mkString(","))
    val stat = new Statistics(r,models.map(_.getName()))
    var sheet = stat.generateCMSheet(spark,param.CMSheetPath)
    var start = System.currentTimeMillis()
    stat.generateAlgoStatisticsByChl(spark,param.RulesChannelStatisticsPath)
    println(s"transforming running time + RulesChannelStatistics is ${(System.currentTimeMillis()-start)/1000} seconds")
    start = System.currentTimeMillis()
    stat.generateAlgoStatistics(spark,param.RulesStatistisPath)
    println(s"RulesStatistis running time is ${(System.currentTimeMillis()-start)/1000} seconds")
    start = System.currentTimeMillis()
    stat.generateCorrelation(spark,param.CorrelationChlPath,param.CorrelationPath)
    println(s"CorrelationChl,Correlation running time is ${(System.currentTimeMillis()-start)/1000} seconds")

    ret = ret.filter{ row=>
      val finalStatus = row.getValuesMap[Int](models.map(_.getName())).values
      val status = finalStatus.max
      if(status<=CheckStatus.real.id)
        true
      else if((status==CheckStatus.anomaly.id)&&
        (finalStatus.filter(_==CheckStatus.anomaly.id).size<3))
        true
      else
        false
    }

    start = System.currentTimeMillis()
//    val (_,modelL) = startAntiSpam(spark,ret,"quality_group")
    val model = CTRModel.load(PathHelper.getPath(param.modelPath,"CTRBysite#srcg"))
    sheet = new QualityModel2(ret).addQualityCol(sheet,model).cache()
    println(s"quality running time is ${(System.currentTimeMillis()-start)/1000} seconds")
    start = System.currentTimeMillis()
    var middleSheet = new StarLevelModel().StarLevel(sheet,param.starLevelPath,param.day,spark).cache()
    println(s"startlevel time is ${(System.currentTimeMillis()-start)/1000} seconds")
    start = System.currentTimeMillis()
    middleSheet.repartition(1).write.mode(SaveMode.Overwrite).option("header","true").csv(param.cmMiddlePath)
    println(s"middleSheet time is ${(System.currentTimeMillis()-start)/1000} seconds")
    start = System.currentTimeMillis()
    maxSheets.repartition(1).write.mode("overwrite").option("header","true").csv(param.CMSheetPath)
    println(s"maxSheets time is ${(System.currentTimeMillis()-start)/1000} seconds")
//    executor.shutdown()
//    executor.awaitTermination(20*60*1000, TimeUnit.MILLISECONDS) // wait 20分钟
    println("total last %d seconds".format(System.currentTimeMillis()/1000-begin/1000))
    ret
  }

  import java.net.URI

  class MergeOldSystem(val spark:SparkSession) {

    val channelStatFile = s"channel.stat.${param.day}"
    val starLevelFile = s"starlevel.${param.day}"
    val channelStatInter = s"channel.stat.will.online.${param.day}"
    val starLevelInter = s"starlevel.will.online.${param.day}"
    val channelStatPath = PathHelper.getPath(config.getString("ChannelStatPath"),channelStatFile)
    val starLevelPath = PathHelper.getPath(config.getString("StarLevelPath"),starLevelFile)
    val channelStatTempPath  = PathHelper.getPath(config.getString("ChannelStatTempPath"),channelStatInter)
    val starLevelTempPath = PathHelper.getPath(config.getString("StarLevelTempPath"),starLevelInter)

    val conf = new Configuration
    val hdfs : FileSystem = FileSystem.get(URI.create(channelStatPath), conf)

    val validPos = 4
    val searchNumPos = 13
    println(s"from ${channelStatTempPath}=======> \n\t\t ${channelStatPath}")
    println(s"from ${starLevelTempPath}=======> \n\t\t ${starLevelPath}")

    def mergeChannelStat(cmSheet: Map[String, (String,String)]) = {
      waitFileExist(s"${channelStatTempPath}.md5")
      val targetPathMd5 = s"${channelStatPath}.md5"
      val inputStream : FSDataInputStream = hdfs.open(new Path(channelStatTempPath))
      if(HdfsHelper.exists(hdfs,new Path(channelStatPath)))
        HdfsHelper.deleteFile(hdfs,channelStatPath)
      val outputStream : FSDataOutputStream = hdfs.create(new Path(channelStatPath))
      var bufferedReader: BufferedReader = null
      var bufferedWriter:BufferedWriter = null

      try {
        //转成缓冲流
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
        bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream))
        //一次读取一行
        var lineTxt: String = bufferedReader.readLine()
        var count = 0
        while (lineTxt != null) {
          val splits = lineTxt.split("\t")
          val key = splits.take(4).mkString(Utils.sep)
          if(cmSheet.get(key)!=None){
            splits(validPos) = cmSheet.get(key).get._2
            splits(searchNumPos)=cmSheet.get(key).get._1
            count = count+1
          }
          bufferedWriter.write(splits.mkString("\t"))
          bufferedWriter.newLine()
          lineTxt = bufferedReader.readLine()
        }
        println(s"in channel stat replace ${count} rows, cmsheet have ${cmSheet.size} rows")
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (bufferedReader != null)
          bufferedReader.close()
        if(bufferedWriter!=null)
          bufferedWriter.close()
      }
      if(HdfsHelper.exists(hdfs,new Path(targetPathMd5)))
        HdfsHelper.deleteFile(hdfs,targetPathMd5)
      val outputStreamMd5: FSDataOutputStream = hdfs.create(new Path(targetPathMd5))
      var bufferedWriterMd5:BufferedWriter = null
      var ins:FSDataInputStream = null
      try{
        bufferedWriterMd5 = new BufferedWriter(new OutputStreamWriter(outputStreamMd5))
        ins = hdfs.open(new Path(channelStatPath))
        val md5Str = DigestUtils.md5Hex(ins)
        bufferedWriterMd5.write(md5Str)
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if(bufferedWriterMd5!=null)
          bufferedWriterMd5.close()
        if(ins!=null)
          ins.close()
      }
    }

    def mergeStarLevel(map: Map[String, String]) = {
      waitFileExist(s"${starLevelTempPath}.md5")
      println(s"all star level rows is ${map.size}")
      val levelMap = new mutable.HashMap[String,String]()
      levelMap++=map
      val targetPathMd5 = s"${starLevelPath}.md5"
      val inputStream : FSDataInputStream = hdfs.open(new Path(starLevelTempPath))
      if(HdfsHelper.exists(hdfs,new Path(starLevelPath)))
        HdfsHelper.deleteFile(hdfs,starLevelPath)
      val outputStream : FSDataOutputStream = hdfs.create(new Path(starLevelPath))
      var bufferedReader: BufferedReader = null
      var bufferedWriter:BufferedWriter = null

      try {
        //转成缓冲流
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
        bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream))
        //一次读取一行
        var lineTxt: String = bufferedReader.readLine()
        var replace = 0
        var added = 0
        while (lineTxt != null) {
          val splits = lineTxt.split("\t")
          val key = splits(0)
          if(levelMap.get(key)==None){
            bufferedWriter.write(lineTxt)
            bufferedWriter.newLine()
          }else{
            splits(1)=levelMap.remove(key).get
            val line = splits.mkString("\t")
            bufferedWriter.write(line)
            bufferedWriter.newLine()
            replace = replace+1
          }
          lineTxt = bufferedReader.readLine()
        }
        println(s"in starLevel merge ${replace} rows, starLevelMap have ${map.size} rows")
        if(levelMap.size>0){
          println(s"in starLevel append ${levelMap.size} rows")
          levelMap.foreach{  case(key,value)=>
            bufferedWriter.write(s"${key}\t${value}")
            bufferedWriter.newLine()
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (bufferedReader != null)
          bufferedReader.close()
        if(bufferedWriter!=null)
          bufferedWriter.close()
      }
      if(HdfsHelper.exists(hdfs,new Path(targetPathMd5)))
        HdfsHelper.deleteFile(hdfs,targetPathMd5)
      val outputStreamMd5: FSDataOutputStream = hdfs.create(new Path(targetPathMd5))
      var bufferedWriterMd5:BufferedWriter = null
      var ins:FSDataInputStream = null
      try{
        bufferedWriterMd5 = new BufferedWriter(new OutputStreamWriter(outputStreamMd5))
        ins = hdfs.open(new Path(starLevelPath))
        val md5Str = DigestUtils.md5Hex(ins)
        bufferedWriterMd5.write(md5Str)
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if(bufferedWriterMd5!=null)
          bufferedWriterMd5.close()
        if(ins!=null)
          ins.close()
      }
    }
    def runStarLevel():Unit ={
      val starLevel = spark.read.option("header","true").csv(param.starLevelPath)
        .rdd.map{ row=>
        (Utils.getRowString(row,0),Utils.getRowString(row,1))
      }.collect().toMap
      mergeStarLevel(starLevel)
      println("map starLevel is done")
    }

    def runChannelStat(): Unit = {
      //      toDF("channel","ctype","jt","sadspace",Statistics.searchNum,"valid")
      val cmSheet=spark.read.option("header","true").csv(param.CMSheetPath)
        .rdd.map{row =>
        val keyBuf = Array("channel","ctype","jt","sadspace").map(Utils.getRowString(row,_))
//        for(i<-0 to 3)
//          keyBuf.append(Utils.getRowString(row,i))
        (keyBuf.mkString(Utils.sep),(row.getAs[String]("searchNum").toDouble.toInt.toString
          ,row.getAs[Int]("valid").toString))
      }.collect().toMap
      println("map cmSheet is done")
      mergeChannelStat(cmSheet)
    }
    def waitFileExist(filePath:String):Unit = {
      var count = 0
      println(HdfsHelper.exists(hdfs,filePath))
      while(HdfsHelper.exists(hdfs,filePath)==false){
        Thread.sleep(30*1000)//
        if(count%10==0)
          println(s"${filePath} is not ready")
        count = count+1
      }
    }
  }
}