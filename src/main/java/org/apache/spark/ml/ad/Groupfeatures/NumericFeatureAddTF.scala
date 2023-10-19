package org.apache.spark.ml.ad.Groupfeatures

import com.typesafe.config.Config
import org.apache.spark.ml.ad.SimpleExpression
import org.apache.spark.ml.common.Utils.getInputColData
import org.apache.spark.ml.common.{AggregateByKey, AntiSpamAlgorithm, AntiSpamModel}
import org.apache.spark.ml.param.{Param, ParamMap, Params, StringArrayParam}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait NumericFeatureAddTFParam extends Params {
  val specifyTp = new StringArrayParam(this,"specifyTp",doc = "")
  val NumericTypeFlag = new Param[Boolean](this,"NumericTypeFlag","NumericTypeFlag")
}

class NumericFeatureAddTF(override val uid: String) extends AntiSpamModel[NumericFeatureAddTF] with AntiSpamAlgorithm with AggregateByKey with NumericFeatureAddTFParam {

  override def setConfig(c: Config): this.type = {
    super.setConfig(c)
    if(c.hasPath("specifyTp")){
      set(specifyTp->c.getStringList("specifyTp").asScala.toArray)
    }else{
      set(specifyTp-> Array[String]())
    }
    if(c.hasPath("NumericTypeFlag")){
      set(NumericTypeFlag->c.getBoolean("NumericTypeFlag"))
    }else{
      set(NumericTypeFlag->false)
    }
    this
  }

  override def copy(extra: ParamMap): NumericFeatureAddTF = ???

  override def transform(dataset: Dataset[_]): DataFrame = {
    val addNumericFeature = udf{
      (statMap:Map[String,Map[String,Map[String,Double]]],itemName:String)=>
        val actMap: collection.Map[String, Map[String, Double]] = {
          val opt = statMap.get(${name})
          if(opt!=None) opt.get
          else {
            println(s"in NumericFeatureAddTF find err algoname is   -->" + ${name})
            println(statMap.keys.mkString(", "))
            new mutable.HashMap[String, Map[String, Double]]()
          }
        }
        try {
          actMap(itemName).getOrElse(indexName, 0.0)
        }catch{
          case e:Throwable=>
            println("not find " + itemName)
            println(actMap.keys.mkString(", "))
            e.printStackTrace()
            0.0
        }
    }

    def getAddColumnsFunction(inputCols:List[String],funName:UserDefinedFunction,additionName:String="") ={
      val add_info = new ListBuffer[Any]
      if(additionName.isEmpty)
        inputCols.foreach(item=>{
          add_info.append(funName(col(AntiSpamAlgorithm.statColName),lit(item)))
        })
      else{
        inputCols.foreach(item=>{
          add_info.append(funName(col(AntiSpamAlgorithm.statColName),lit(item),lit(additionName)))
        })
      }
      add_info.asInstanceOf[mutable.Seq[Column]]
    }
    val outputs = ${outputCols}.toSeq
    dataset.withColumns(outputs,getAddColumnsFunction(${inputCols}.toList,addNumericFeature))

  }




  val indexName = "Count"
  override def seqOp(statMap: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]], row: Row): mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]] = {


    ${inputCols}.foreach(itemFull=>{
      val itemFullList = itemFull.split("\\|")
      val item = itemFullList.head.toString
      val feaMap = getStatMap(statMap,itemFull)
      val fea = getInputColData(row,item)
      if (fea.nonEmpty) if(itemFullList.length ==1) fea.foreach(num => {
        val value =  if (${NumericTypeFlag}) num.toString.toDouble else 1.0
        if(!${specifyTp}.isEmpty){
          if (${specifyTp}.contains(row.getAs[String]("tp")))
            feaMap.put(indexName, value + feaMap.getOrElse(indexName, 0.0))
        }
        else{
          feaMap.put(indexName, value + feaMap.getOrElse(indexName, 0.0))
        }
      })
      else{
        val valueMap = new mutable.HashMap[String, ListBuffer[Any]]()
        var conditionfealist = new ListBuffer[Boolean]
        val exprFlag = SimpleExpression.parse(itemFullList(1)).getValue(row, valueMap)
        conditionfealist = SimpleExpression.parse(itemFullList(1)).compareFullList(valueMap)
        if (fea.size==conditionfealist.size) {
          val conditionRes = conditionfealist.zip(fea)
          conditionRes.foreach(info => {
            if (info._1)
            {
              val value =  if (${NumericTypeFlag}) info._2.toString.toDouble else 1.0
              if(!${specifyTp}.isEmpty){
                if (${specifyTp}.contains(row.getAs[String]("tp")))
                  feaMap.put(indexName, value + feaMap.getOrElse(indexName, 0.0))
              }
              else{
                feaMap.put(indexName, value + feaMap.getOrElse(indexName, 0.0))
              }
            }
          })
        }
        else if(conditionfealist.contains(true)) fea.foreach(info=>{
          val value =  if (${NumericTypeFlag}) info.toString.toDouble else 1.0
          if(!${specifyTp}.isEmpty){
            if (${specifyTp}.contains(row.getAs[String]("tp")))
              feaMap.put(indexName, value + feaMap.getOrElse(indexName, 0.0))
          }
          else
            feaMap.put(indexName, value + feaMap.getOrElse(indexName, 0.0))
        })
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
