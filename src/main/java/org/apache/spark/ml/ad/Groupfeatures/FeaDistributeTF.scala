package org.apache.spark.ml.ad.Groupfeatures

import com.typesafe.config.Config
import org.apache.spark.ml.ad.SimpleExpression
import org.apache.spark.ml.common.{AggregateByKey, AntiSpamAlgorithm, AntiSpamModel}
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap, Params, StringArrayParam}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.common.Utils.getInputColData
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

trait FeaDistributeTFParam extends Params {
  val itemNumLimit = new Param[Int](this,"itemNumLimit","定义分布项最大长度,防止内容过多")
  val itemNumBinLimit = new Param[Int](this,"itemNumBinLimit","定义bin分布项最大长度,防止内容过多")
//  val isBin = new Param[Boolean](this,"isBin","是否统计分bin信息")
  val binStyle = new Param[Map[String,String]](this,"binStyle","分bin的样式")
  val expr = new Param[String](this,"expr","定义条件")
  val isBernoulli  = new BooleanParam(this,"isBernoulli"," 是否伯努利化")
  val validKeys = new StringArrayParam(this,"validKeys",doc = "")
  val excludeKeys = new StringArrayParam(this,"excludeKeys","")
  setDefault(excludeKeys->Array("null"))
}

class FeaDistributeTF(override val uid: String) extends AntiSpamModel[FeaDistributeTF] with AntiSpamAlgorithm with AggregateByKey with FeaDistributeTFParam{
  override def setConfig(c: Config): this.type = {
    super.setConfig(c)
    if(c.hasPath("itemNumLimit"))
      set(itemNumLimit->c.getInt("itemNumLimit"))
    else
      set(itemNumLimit->0)
    if(c.hasPath("itemNumBinLimit"))
      set(itemNumBinLimit->c.getInt("itemNumBinLimit"))
    else
      set(itemNumBinLimit->0)
    if(c.hasPath("expr"))
      set(expr->c.getString("expr"))
    else
      set(expr->"noexpr")
    if (c.hasPath("binStyle"))
      set(binStyle->c.getConfig("binStyle").root.keySet.map(k=>(k,c.getConfig("binStyle").getString(k))).toMap)
    else
      set(binStyle->Map())
    if(c.hasPath("isBernoulli"))
      set(isBernoulli->c.getBoolean("isBernoulli"))
    else
      set(isBernoulli->false)
    if(c.hasPath("validKeys"))
      set(validKeys->c.getStringList("validKeys").asScala.toArray)
    else
      set(validKeys->Array[String]())
    if(c.hasPath("excludeKeys"))
      set(excludeKeys->c.getStringList("excludeKeys").asScala.toArray)
    else
      set(excludeKeys->Array("null"))
    this
  }

  override def copy(extra: ParamMap): FeaDistributeTF = ???

  override def transform(dataset: Dataset[_]): DataFrame = {
    val validKeySet = ${validKeys}.toSet
      val addDistribute = udf{
        (statMap:Map[String,Map[String,Map[String,Double]]])=>
          val opt = statMap.get(${name})
          var feaRes = ""
          var feaBinRes = ""
          var feaRatio = 0.0
          var feaSum = 0.0
          if(opt!=None)
            if (opt.get.get(feaAddColumn)!=None){
              val fea = opt.get.get(feaAddColumn).get
              if (fea.size > 0){
                var feaSortArr = fea.toArray.sortWith(_._2 > _._2)
                var feaSortArrTop = feaSortArr
                feaSum = feaSortArr.map(k=>k._2).sum
                feaRatio = feaSortArr(0)._2 / feaSum
                if (${itemNumLimit}!= 0 && feaSortArrTop.length > ${itemNumLimit}){
                  feaSortArrTop = feaSortArrTop.slice(0,${itemNumLimit})
                }
                val feaRes_temp = feaSortArrTop.map{item =>
                  var str = item._1.replaceAll("[\\s|\\||\\#|\\:|,]*","")
                  if(str.length<1)
                    str = "unk"
                  if(validKeySet.isEmpty==false){
                    if(validKeySet.contains(str)==false)
                      str = "other"
                  }

                  var fre = item._2.toInt
                  if(${isBernoulli})
                    fre = 1
                  (str ,fre)
              }
                feaBinRes = feaRes_temp.map(item=>"%s:%.3f".format(item._1,item._2/feaSum)) .mkString("|")
                feaRes = feaRes_temp.map(item=>item._1+":"+item._2) .mkString("|")
                // 开始计算分bin统计
                if (${binStyle}.nonEmpty){
                  val res = feaSortArr.map(k=>k._2.toInt)
                  var countMap = new mutable.HashMap[Int,Int]()
                  val keyname = ${binStyle}.keySet.head
                  var value = new ListBuffer[Int]
                  if (keyname == "custom"){
                    ${binStyle}.get(keyname).get.split(",").foreach(k=>value.append(k.toInt))
                  }
                  else{
                    val bin = ${binStyle}.get(keyname).get.split("-")(0).toInt
                    val resMax = ${binStyle}.get(keyname).get.split("-")(1).toInt
                    val middle = resMax / bin
                    value.append(1)
                    for (cas <- 1 until bin+1){
                      value.append(middle * cas)
                    }
                  }
//                  for (cas <- res){
//
//                  }
                  for (bucket <- value){
                    val count = res.filter(_ <= bucket.toInt).length
                    countMap.put(bucket, count)
                  }
                  val arr = countMap.toArray.map(k=>(k._1,k._2)).sortWith(_._1 < _._1)
                  val arr1 = arr.tail
                  val secondsMapNew = arr.zip(arr1).map(k=>(k._2._1,k._2._2-k._1._2)).toMap
                  for (cas <- secondsMapNew){
                    countMap.put(cas._1,cas._2)
                  }
                  countMap.put(value.last + 1,res.filter(_ > value.last).length)
                  var sortArr = countMap.toArray.filter(_._2>0).sortWith(_._1 > _._1).map(k=>(k._1,k._2))
                  if (${itemNumBinLimit}!= 0 && sortArr.length > ${itemNumBinLimit}){
                    sortArr = sortArr.slice(0,${itemNumBinLimit})
                  }
                  feaBinRes = sortArr.map(item =>
                    item._1 + ":" + item._2
                  ).mkString("|")
                }
              }
            }
          (feaRes, feaRatio, feaSum, feaBinRes)
      }
    val outputs = ${outputCols}.toSeq
    var res = addDistribute(col(AntiSpamAlgorithm.statColName))
    var df = dataset.withColumn("outputs",res)
    outputs.zipWithIndex.foreach(x => {
      val index = x._2 + 1
      df = df.withColumn(x._1, col(s"outputs._$index"))
    })
    df = df.drop("outputs")
    df
    }

  val feaAddColumn = "feaDistributeColumn"
  override def seqOp(statMap: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]], row: Row): mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]] = {
    var resIndex = new ListBuffer[Int]
    if (${expr}!="noexpr"){
      val valueMap = new mutable.HashMap[String, ListBuffer[Any]]()
      val exprFlag = SimpleExpression.parse(${expr}).getValue(row, valueMap)
      if (exprFlag)
        return statMap
      else {
        resIndex = SimpleExpression.parse(${expr}).compare(valueMap)
        if(resIndex.isEmpty)
          return statMap
      }
    }

    val feaMap = getStatMap(statMap,feaAddColumn)
    ${inputCols}.foreach(item =>{
        var content = new mutable.ListBuffer[(Any,Int)]()
        val res = getInputColData(row,item)
        if (${expr}!="noexpr"){
          if (${expr}.contains(".")){
            content = res.zipWithIndex.filter(k=>resIndex.contains(k._2))
          }
          else {
            content = res.zipWithIndex
          }
          content.foreach(info=>{
            if(info._1!=null)
              feaMap.put(info._1.toString, 1.0 + feaMap.getOrElse(info._1.toString, 0.0))
          })
        }
        else{
          res.foreach(info=>{
            if(info!=null)
              feaMap.put(info.toString, 1.0 + feaMap.getOrElse(info.toString, 0.0))
          })
        }
    })
    statMap
  }

  override def combOp(map1: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]], map2: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]]): mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]] = {
    val m1 = getStatMap(map1,feaAddColumn)
    val m2 = getStatMap(map2,feaAddColumn)
    for((name,num) <- m2)
      m1.put(name,num + m1.getOrElse(name,0.0))
    map1
  }

}
