package org.apache.spark.ml.common

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
trait Holiday extends Params{
  val holidayFileAddr = new  Param[String](this,"holidayFileAddr","holidayFileAddr")
  setDefault(holidayFileAddr-> "vocation.csv")
  var Holidays = mutable.HashSet[String]()
  var workdays = mutable.HashSet[String]()
  def isWeekend(dateStr: String): Boolean = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = sdf.parse(dateStr)
    val cal = Calendar.getInstance();
    cal.setTime(date);

    var w = cal.get(Calendar.DAY_OF_WEEK) - 1;
    //星期天 默认为0
    if (w <= 0)
      w = 7
    w>=6
  }
  def readFile()={
    val spark = SparkSession.builder().getOrCreate()
    val frame = spark.read.option("header",true).csv(${holidayFileAddr})
    def init(name:String)= {
      frame.filter(expr(s"type='${name}'")).rdd.flatMap { r: Row =>
        val start = r.getAs[String]("start")
        val last = r.getAs[String]("last").toInt
        Array.range(0, last).map { step =>
          Utils.getAnotherYYYYMMDD(start, step)
        }
      }.collect()
    }
    Holidays ++= init("holiday")
    workdays ++= init("workday")
  }
  def isHoliday(day:String):Boolean={
    if(Holidays.contains(day)) true
    else if(workdays.contains(day)) false
    else if(isWeekend(day)) true
    else false
  }

}
