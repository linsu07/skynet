package org.apache.spark.ml.common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.google.gson.Gson
import com.typesafe.config.Config
import org.apache.spark.ml.ad.InfoFromDetailArryJsonHelper.getFromRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object Utils {
  val sep = "#"
  val pattern = new Regex("([\u4e00-\u9fa5]+)|([0-9]+)|([a-z]+)|([A-Z]+)") //匹配的20902个基本汉字，数字以及大小写字母

  def getPos(fieldName: String, schema: StructType): Int = {
    var i = 0
    for (field <- schema.fields) {
      if (fieldName.equals(field.name.trim))
        return i
      i += 1
    }
    i
  }

  def toTimeDesc(t:Long):String = {
    val calendar = Calendar.getInstance()
    val df = new SimpleDateFormat("MM/dd HH:mm:ss")
    val curday = new Date(t)
    calendar.setTime(curday)
    df.format(calendar.getTime)
  }
  def getAnotherYYYYMMDD(curYYYYMMDD: String, step: Int): String = {

//    val df = new SimpleDateFormat("YYYYMMdd")
//    val calendar = Calendar.getInstance()
//    var year = curYYYYMMDD.substring(0, 4).toInt - 1900
//    var mon = curYYYYMMDD.substring(4, 6).toInt - 1
//    var d = curYYYYMMDD.substring(6).toInt
//    val curday = new Date(year, mon, d)
//
//    calendar.setTime(curday)
//    calendar.add(Calendar.DAY_OF_MONTH, step)
//    df.format(calendar.getTime)
    val formatter = DateTimeFormat.forPattern("yyyyMMdd");
    val dateTime = formatter.parseDateTime(curYYYYMMDD);
    val newDate = dateTime.plusDays(step).toString("yyyyMMdd")
    newDate
  }


  def str2map(str:String):mutable.HashMap[String,Int] ={
    val tempMap = new mutable.HashMap[String,Int]()
    if(str != ""){
      val kv: Array[Array[String]] = str.split("\\|").map(_.split(":"))
      kv.foreach( item =>
        if(item.length == 2)
          tempMap += (item(0)->item(1).toInt))
    }
    tempMap
  }


  /**
   * 返回两个日期的天数差
   * @param startYYYYMMDD
   * @param endYYYYMMDD
   * @return
   */
  def getYYYYMMDDGapDay(startYYYYMMDD: String, endYYYYMMDD: String): Long = {

    val pattern = "yyyyMMdd"
    val startSdf = new SimpleDateFormat(pattern).parse(startYYYYMMDD)
    val endSdf = new SimpleDateFormat(pattern).parse(endYYYYMMDD)
    (endSdf.getTime - startSdf.getTime) / 1000 / 3600 / 24
  }


  def getRowString(row: Row, pos: Int): String = {
    val raw = row.get(pos)
    var ret = ""
    if (raw != null)
      ret = raw.toString
    ret
  }

  def getRowInt(row:Row,pos:Int):Int = {
    val raw = row.get(pos)
    if(raw==null) 0
    raw.toString.toDouble.toInt
  }

  def getRowString(row: Row, name: String): String = {
    getRowString(row, getPos(name, row.schema))
  }

  def sigmoid(inX: Double): Double = {
    val y = 1.0 / (1 + math.exp(-inX))
    y
  }

  def isDigital(s: String): Boolean = {
    if(s==null) return false
    for (i <- 0 until s.length) {
      val c = s.charAt(i)
      if ((Character.isDigit(c) == false)&&(c.equals('.')==false))
        return false
    }
    if (s.length == 0)
      return false
    return true
  }

  def getFilterSymbolsString(s: String): String = {
    pattern.findAllIn(s).mkString("")
  }


  def getInputColData(row:Row,item:String) = {
    val indexInfo = item.split("\\.")
    var fea = new ListBuffer[Any]
    if (indexInfo.length == 1) {
      val itemType = row.schema.fields(row.schema.fieldIndex(item))
      if(itemType.dataType.toString == "StringType")
      {
        val info = row.getAs[String](item)
        fea.append(info)
      }
      else if (itemType.dataType.toString == "ArrayType(StringType,true)")
      {
        val info = row.getAs[Seq[String]](item)
        fea.appendAll(info)
      }
      else
      {
        try {
          val info = row.getAs[Any](item)
          if(info!=null)
            fea.append(info.toString)
          else
            fea.append(null)
        }catch {
          case e:Throwable=>fea.append("0")
        }
      }
    }
    else {
      fea.appendAll(getFromRow(row, item: String))
    }
    fea
  }

  def main(args: Array[String]): Unit = {
    //1601604290000, 1601634731000
//    val t = 1601634731000L
//    println(Utils.toTimeDesc(t))
//    val a = getYYYYMMDDGapDay("20201028","20201101")
    val a = str2map("114.250.247.48:10|111.197.63.36:1|123.116.242.45:1")
    println(a)
  }


}
