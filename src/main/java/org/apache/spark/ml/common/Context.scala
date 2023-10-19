package org.apache.spark.ml.common

import scala.collection.mutable

class Context extends Serializable {
  val ChannelStat = "ChannelStat"
  var product:String = _
  var curDay:String = _
  var modelPath:String = _
  var attributes = new mutable.HashMap[String,Any]()
  var standardModelPath:String ="unknown"
  var dayWindow:Int = _
  var cmsheetsPaths:String = _
  var outputRoot:String = _
  var inputRoot:String = _
  var modelParquetPath:String = _
  def setOutputRoot(path:String)={
    outputRoot = path
    this
  }

  def setIutputRoot(path:String)={
    inputRoot = path
    this
  }

  private val checkAlgos = new mutable.HashMap[String,mutable.HashSet[String]]()
  def setGroupCheckAlgos (groupName:String,algoName:String)={
    checkAlgos.getOrElseUpdate(groupName,new mutable.HashSet[String]()).add(algoName)
  }
  def getCheckList(groupName:String):Array[String]={
    checkAlgos.getOrElseUpdate(groupName,new mutable.HashSet[String]()).toArray
  }
//  def getCurCheckList():Array[String]={
//    checkAlgos.head._2.toArray
//  }
//  def retsetGroupCheckAlgos()={
//    checkAlgos.clear()
//  }
  def setStandardModelPath(path:String) = {
    standardModelPath = path
    this
  }
  def setCurDay(day:String) = {
    curDay = day
    this
  }
  def setProduct(pro:String) = {
    product = pro
    this
  }
  def setModelPath (path:String) = {
    modelPath = path
    this
  }
  def setDays(days:Int)={
    this.dayWindow = days
    this
  }
  def setCMSheetsPath(path:String)={
    this.cmsheetsPaths = path
    this
  }
  def setModelParquetPath(path:String)={
    this.modelParquetPath = path
    this
  }
}

object Context{
  val groupScoreKey = "groupScore"
}
