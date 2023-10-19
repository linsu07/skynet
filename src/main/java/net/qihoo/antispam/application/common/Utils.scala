package net.qihoo.antispam.application.common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.typesafe.config.Config
import org.apache.spark.ml.common.AntiSpamAlgorithm
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Utils {
  def getInstance(c: Config,day:String=null): Any = {
    val clsName = c.getString("class")
    val clazz = Class.forName(clsName)
    if(c.hasPath("model_addr")){
      var modelAddr = c.getString("model_addr")
      if(day!=null)
        modelAddr = modelAddr.replace("datestr",day)
      val loadM = clazz.getDeclaredMethod("load",classOf[String])
      loadM.invoke(null,modelAddr)
    }else{
      val algoName = AntiSpamAlgorithm.getName(c)
      clazz.getConstructor(classOf[String]).newInstance(algoName)
    }
  }
  def main(args:Array[String]): Unit ={
    println("hello,world")
    println(parseArgs(Array("a=1","b=hello")))
    println(getAnotherYYYYMMDD("20121230",3))
  }
  def getAnotherYYYYMMDD(curYYYYMMDD: String, step: Int): String = {

    val df = new SimpleDateFormat("yyyyMMdd")
    val d = df.parse(curYYYYMMDD)
    //println(d)
    val calendar = Calendar.getInstance()
//    var year = curYYYYMMDD.substring(0, 4).toInt - 1900
//    var mon = curYYYYMMDD.substring(4, 6).toInt - 1
//    var d = curYYYYMMDD.substring(6).toInt
//    val curday = new Date(year, mon, d)
//    calendar.setTime(curday)
//    val calendar= df.getCalendar
    calendar.setTime(d)
    calendar.add(Calendar.DAY_OF_MONTH, step)
    df.format(calendar.getTime)

  }

  def tranTimeStampToTimeString(tm:String) :String={
    val fm = new SimpleDateFormat("yyyyMMdd")
    val totime = fm.format(new Date(tm.toLong))
    totime
  }

  def arrayType2String(df:DataFrame):DataFrame={
    val fields =  df.schema.filter(_.dataType.typeName.startsWith("array"))
    var ret:DataFrame = df
    val array2Str = udf{
        (arr: Seq[Any])=>{
          arr.mkString("; ")
        }
    }
    fields.foreach{field=>
      ret = ret.withColumn(field.name,array2Str(col(field.name)))
    }
    ret
  }


  def getNextDayList(day: String, number: Int): Array[String] = {
    val ret = new ListBuffer[String]()
    ret.append(day)
    for (i <- 1 until number) {
      ret.append(getAnotherYYYYMMDD(day, i))
    }
    ret.toArray
  }
  private val SYMBOL_EQUAL = '='

//  def parseArgs(args: Array[String]): mutable.HashMap[String, String] = {
//    val argsMap: mutable.HashMap[String, String] = mutable.HashMap()
//    args.foreach(v => {
//      val index = v.indexOf(SYMBOL_EQUAL)
//      require(index > 0, s"Command line argument error: $index, can't find = in argument" + v)
//      val key = v.substring(0, index).trim
//      val value = v.substring(index + 1, v.length).trim
//
//      argsMap.put(key, value)
//    })
//    argsMap
//  }
  def parseArgs(args: Array[String]): Map[String, String] = {
      args.map{ segment=>
        require(segment.contains("="),s"param ${segment} has no =, check it!")
        val kv = segment.split("=").map(_.trim)
        (kv(0),kv(1))
      }.toMap
  }
}
