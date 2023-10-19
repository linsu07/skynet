package org.apache.spark.ml.ad.Groupfeatures

import com.google.gson.{JsonArray, JsonElement}
import com.typesafe.config.Config
import org.apache.avro.generic.GenericData.StringType
import org.apache.hadoop.hdfs.util.Diff.ListType
import org.apache.spark.ml.common.{AggregateByKey, AntiSpamAlgorithm, AntiSpamModel}
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, ParamValidators, Params}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.ml.ad.InfoFromDetailArryJsonHelper.getFromRow
import org.apache.spark.ml.ad.SimpleExpression
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.ml.common.Utils.getInputColData

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.parsing.json.JSONObject




trait DiscreteFeatureMultiInputColsAddTFParam extends Params {
  val MaxValue = new Param[Boolean](this,"MaxValue","every details specific info")
  val MinValue = new Param[Boolean](this,"MinValue","every details specific info")
  val MeanValue = new Param[Boolean](this,"MeanValue","every details specific info")
  val DevValue = new Param[Boolean](this,"DevValue","every details specific info")
}

class DiscreteFeatureMultiInputColsAddTF(override val uid: String) extends AntiSpamModel[DiscreteFeatureAddTF] with AntiSpamAlgorithm with AggregateByKey with DiscreteFeatureMultiInputColsAddTFParam{
  override def setConfig(c: Config): this.type = {
    super.setConfig(c)
    if(c.hasPath("MaxValue"))
      set(MaxValue->c.getBoolean("MaxValue"))
    else
      set(MaxValue->false)

    if(c.hasPath("MinValue"))
      set(MinValue->c.getBoolean("MinValue"))
    else
      set(MinValue->false)

    if(c.hasPath("MeanValue"))
      set(MeanValue->c.getBoolean("MeanValue"))
    else
      set(MeanValue->false)

    if(c.hasPath("DevValue"))
      set(DevValue->c.getBoolean("DevValue"))
    else
      set(DevValue->false)
    this
  }

  override def copy(extra: ParamMap): DiscreteFeatureAddTF = ???

  override def transform(dataset: Dataset[_]): DataFrame = {
      val addDiscreteFeature = udf{
        (statMap:Map[String,Map[String,Map[String,Double]]],addname:String)=>
          val actMap: collection.Map[String, Map[String, Double]] = {
            val opt = statMap.get(${name})
            if(opt!=None) opt.get
            else
              new mutable.HashMap[String,Map[String,Double]]()
          }
          actMap(addname).keys.size
      }

    def getAddColumnsFunction(funName:UserDefinedFunction,additionName:String="") ={
      val add_info = new ListBuffer[Any]
      if(additionName.isEmpty)
        add_info.append(funName(col(AntiSpamAlgorithm.statColName),lit(${outputCols}(0))))
      else{
          add_info.append(funName(col(AntiSpamAlgorithm.statColName),lit(lit(${outputCols}(0)),lit(additionName))))
      }
      add_info.asInstanceOf[mutable.Seq[Column]]
    }

    val addNumValue: UserDefinedFunction = udf{
      (statMap:Map[String,Map[String,Map[String,Double]]],addname:String,funflag:String)=>
        val actMap: collection.Map[String, Map[String, Double]] = {
          val opt = statMap.get(${name})
          if(opt!=None) opt.get
          else
            new mutable.HashMap[String,Map[String,Double]]()
        }

        var result = -1.0
        if( actMap(addname).values.nonEmpty) {
          if (funflag.toLowerCase().contains("max"))
            result = actMap(addname).values.max.toDouble
          else if (funflag.toLowerCase().contains("min"))
            result = actMap(addname).values.min.toDouble
          else if (funflag.toLowerCase().contains("mean"))
            result = actMap(addname).values.sum / actMap(addname).values.size.toDouble
          else if (funflag.toLowerCase().contains("dev"))
            {
              val numlist = actMap(addname).values
              val mean = numlist.sum/numlist.size
              result = numlist.map(x=>math.pow(x - mean,2)).sum/numlist.size.toDouble
            }
        }
        else
          result= -1.0

        result
    }

    val outputs = ${outputCols}
    var data1 = dataset.withColumns(outputs,getAddColumnsFunction(addDiscreteFeature))

    if (${MaxValue})
    {
      val outputs1 = ${outputCols}.map(info=> info+"Max").toSeq
      data1 = data1.withColumns(outputs1,getAddColumnsFunction(addNumValue,"max"))
    }
    if (${MinValue})
    {
      val outputs = ${outputCols}.map(info=> info+"Min").toSeq
      data1 = data1.withColumns(outputs,getAddColumnsFunction(addNumValue,"min"))
    }

    if (${MeanValue})
    {
      val outputs = ${outputCols}.map(info=> info+"Mean").toSeq
      data1 = data1.withColumns(outputs,getAddColumnsFunction(addNumValue,"mean"))
    }

    if (${DevValue})
    {
      val outputs = ${outputCols}.map(info=> info+"Dev").toSeq
      data1 = data1.withColumns(outputs,getAddColumnsFunction(addNumValue,"Dev"))
    }
    data1
    }


  override def seqOp(statMap: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]], row: Row): mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]] = {
    val statName = ${outputCols}(0)
    ${inputCols}.foreach(itemFull =>{
      val itemFullList = itemFull.split("\\|")
      val item = itemFullList.head.toString
//      val indexInfo = item.split("\\.")
      val feaMap = getStatMap(statMap,statName)
      if(itemFullList.length==1)
        {
          val feaList = getInputColData(row,item)
//          if((feaList.length>0)&&(itemFull.equals("adclick.hour")))
//            feaList.foreach(fea=>{
//              feaMap.put(fea.toString,1.0 +feaMap.getOrElse(fea.toString,0.0))
//            })
          feaList.foreach(fea=>{
            feaMap.put(fea.toString,1.0 +feaMap.getOrElse(fea.toString,0.0))
          })
        }
      else {
        val valueMap = new mutable.HashMap[String, ListBuffer[Any]]()
        var conditionfealist = new ListBuffer[Boolean]
        val se = SimpleExpression.parse(itemFullList(1))
        val exprFlag = se.getValue(row, valueMap)
        conditionfealist = se.compareFullList(valueMap)
        val res =getInputColData(row,item)    //item= ads_details.inudstry
        if (res.size==conditionfealist.size) {
          val conditionRes = conditionfealist.zip(res)
          conditionRes.foreach(info => {
            if (info._1)
              feaMap.put(info._2.toString, 1.0 + feaMap.getOrElse(info._2.toString, 0.0))
          })
        }
        else{
          if(conditionfealist.contains(true))
            res.foreach(info => {
                feaMap.put(info.toString, 1.0 + feaMap.getOrElse(info.toString, 0.0))
            })
        }
      }
    })
    statMap
  }

  override def combOp(map1: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]], map2: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]]): mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]] = {
    val itemKeys = map1.getOrElse(${name},new mutable.HashMap[String,mutable.HashMap[String,Double]]()).keys.toSet.union(map2.getOrElse(${name},new mutable.HashMap[String,mutable.HashMap[String,Double]]()).keys.toSet)
    for (signItem <- itemKeys) {
      val searchMap1 = getStatMap(map1, signItem)
      val searchMap2 = getStatMap(map2, signItem)
      for ((pos, num) <- searchMap2)
        searchMap1.put(pos, num + searchMap1.getOrElse(pos, 0.0))
    }

    map1
  }

}
