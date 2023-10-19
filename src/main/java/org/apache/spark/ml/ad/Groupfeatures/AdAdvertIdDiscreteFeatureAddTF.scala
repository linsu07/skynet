package org.apache.spark.ml.ad.Groupfeatures

/**
  *@author qinsha 20200806
  */
import com.typesafe.config.Config
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.ad.Groupfeatures.{DiscreteFeatureAddTF, DiscreteFeatureAddTFParam}
import org.apache.spark.ml.ad.SimpleExpression
import org.apache.spark.ml.algo.RelativeEntropy
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.ml.common.Utils.getInputColData
import org.apache.spark.ml.common.{AggregateByKey, AntiSpamAlgorithm, AntiSpamModel, Utils}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.{DoubleArrayParam, DoubleParam, IntParam, Param, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction


class AdAdvertIdDiscreteFeatureAddTF(override val uid: String) extends AntiSpamModel[AdAdvertIdDiscreteFeatureAddTF] with AntiSpamAlgorithm with AggregateByKey{

  override def setConfig(c: Config): this.type = {
    super.setConfig(c)
    this
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val addDiscreteFeature = udf{
      (statMap:Map[String,Map[String,Map[String,Double]]],addname:String)=>
        val actMap: collection.Map[String, Map[String, Double]] = {
          val opt = statMap.get(${name})
          if(opt!=None) opt.get
          else
            new mutable.HashMap[String,Map[String,Double]]()
        }
        val maxRate = if( actMap(addname).values.nonEmpty && actMap(addname).values.sum !=0) actMap(addname).values.max.toDouble/actMap(addname).values.sum else 0.0
         val max = if(actMap(addname).values.nonEmpty) actMap(addname).values.max else 0.0
          (actMap(addname).keys.size,max ,maxRate )

    }

    val outputs = ${outputCols}.toSeq
    var data1 = dataset.withColumn("outputs",addDiscreteFeature(col(AntiSpamAlgorithm.statColName),lit(${inputCols}(0))))

    outputs.zipWithIndex.foreach(x => {
      val index = x._2 + 1
      data1 = data1.withColumn(x._1, col(s"outputs._$index"))
    })
    data1 = data1.drop("outputs")

    data1
  }
  override def copy(extra: ParamMap): AdAdvertIdDiscreteFeatureAddTF = ???
  override def seqOp(statMap: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]], row: Row): mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]] = {
    val num = getInputColData(row,${inputCols}(1))
    val itemFullList = ${inputCols}(0).split("\\|")
    val item = itemFullList.head.toString
    //      val indexInfo = item.split("\\.")
    val feaMap = getStatMap(statMap,${inputCols}(0))

    val valueMap = new mutable.HashMap[String, ListBuffer[Any]]()
    var conditionfealist = new ListBuffer[Boolean]
    val se = SimpleExpression.parse(itemFullList(1))
    val exprFlag = se.getValue(row, valueMap)
    conditionfealist = se.compareFullList(valueMap)
    val res =getInputColData(row,item)    //item= ads_details.inudstry

    val conditionRes = conditionfealist.zip(res)
    for(index <- conditionRes.indices)
    {
      if (conditionRes(index)._1)
        feaMap.put(conditionRes(index)._2.toString, num(index).toString.toInt + feaMap.getOrElse(conditionRes(index)._2.toString, 0.0))
    }

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