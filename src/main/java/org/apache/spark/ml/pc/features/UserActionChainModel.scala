package org.apache.spark.ml.pc.features

import com.typesafe.config.Config
import org.apache.spark.ml.common.{AggregateByKey, AntiSpamAlgorithm, AntiSpamModel, Utils}
import org.apache.spark.ml.param.{BooleanParam, IntParam, ParamMap, Params, StringArrayParam}
import org.apache.spark.ml.pc.{AdsHelper, ClickHelper}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, lit, udf}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
//search1:["lm","home_.*","se[0-9|_].*","360.*","corr_.*","hao.*","recom-nlp.*","oneboxlink.*","internal_.*"]
//search2:["srp.*","res-sug.*","sug-local","tab_.*","inline.*"]
//paging:["srp_paging"]
//right_sug:["know_side_nl.*"]
//related:["related_.*","pdr_guide.*","inline-recommend_.*"]
trait UserActionChainParam extends Params{
  val actions = new StringArrayParam(this,"actions","")
}
class UserActionChainModel(override val uid:String) extends AntiSpamModel[UserActionChainModel] with AggregateByKey
with UserActionChainParam{
  override def transform(dataset: Dataset[_]): DataFrame = {
    val addTimelist = udf{
      (statMap:Map[String,Map[String,Map[String,Double]]])=>
        val actMap = {
          val opt = statMap.get(${name})
          if(opt!=None) opt.get
          else
            new mutable.HashMap[String,Map[String,Double]]()
        }
        var actionTimeList = actMap.flatMap{case (action,timeMap)=>
          timeMap.map(_._1).toArray.map{ time=>
            (time.toLong,action)
          }
        }.toArray.sortBy(_._1)
        if(actionTimeList.length>200)
          actionTimeList = actionTimeList.take(200)
        actionTimeList.map(_._1.toString)
    }
    val addActionlist = udf{
      (statMap:Map[String,Map[String,Map[String,Double]]])=>
        val actMap = {
          val opt = statMap.get(${name})
          if(opt!=None) opt.get
          else
            new mutable.HashMap[String,Map[String,Double]]()
        }
        var actionTimeList = actMap.flatMap{case (action,timeMap)=>
          timeMap.map(_._1).toArray.map{ time=>
            (time.toLong,action)
          }
        }.toArray.sortBy(_._1)
        if(actionTimeList.length>200)
          actionTimeList = actionTimeList.take(200)
        actionTimeList.map(_._2)
      }
    val outputs = ${outputCols}.toSeq
    dataset.withColumns(outputs,Seq(addActionlist(col(AntiSpamAlgorithm.statColName))
      ,addTimelist(col(AntiSpamAlgorithm.statColName)))).cache()
  }


  override def setConfig(c: Config): this.type = {
    super.setConfig(c)
    set(actions->Array("search1","search2","paging","right_sug","related"))
    for(act<-${actions}){
      val expr = c.getStringList(act).asScala
      for(ex<-expr)
        act2simple.append((ex,act))
    }
    this
  }
  val act2simple = new ListBuffer[(String,String)]()
  def getTime(t:String,ts:String,defaultTime:Double=(-1.0)): String ={
    val time = {
      if (Utils.isDigital(t)) t
      else if (Utils.isDigital(ts)) ts
      else defaultTime.toLong.toString
    }
    if(time.contains("."))
      time.split("\\.")(0)
    else
      time
  }

  def toSimple(src:String):String = {
    val findopt = act2simple.find{ case (exr,_) =>
      exr.r.findFirstIn(src)!=None
    }
    if(findopt==None)
      s"unk"
    else
      s"${findopt.get._2}"
  }
  override def seqOp(statMap: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]], row: Row): mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]] = {
    if (row.getAs[String]("tp")>"1")//only click
      return statMap

    var src = row.getAs[String](${inputCols}(0))
    val t = row.getAs[String](${inputCols}(1))
    val ts = row.getAs[String](${inputCols}(2))
    val time =getTime(t,ts,0)
    if(time.equals("0")) return statMap
    src = toSimple(src)
    var actionMap = getStatMap(statMap,src)
    actionMap.put(time,1.0)
    val clicks = ClickHelper.getFromRow(row)
    var count = 1
    src = "click"
    actionMap = getStatMap(statMap,src)
    clicks.foreach{clk=>
      val clkTime = getTime(clk.t,clk.ts,time.toDouble+5000.0*count)
      actionMap.put(clkTime,1.0)
      count = count+1
    }
    val adclicks = AdsHelper.getFromRow(row)
    src = "adclick"
    actionMap = getStatMap(statMap,src)
    adclicks.foreach{clk=>
      clk.click_time.foreach { ti=>
        val clkTime = getTime(ti, ti, time.toDouble + 10000.0 * count)
        actionMap.put(clkTime, 1.0)
        count = count + 1
      }
    }
    statMap
  }
  override def combOp(map1: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]], map2: mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]]): mutable.HashMap[String, mutable.HashMap[String, mutable.HashMap[String, Double]]] = {
    val m1 = map1.getOrElseUpdate(${name},new mutable.HashMap[String,mutable.HashMap[String,Double]]())
    val m2 = map2.getOrElseUpdate(${name},new mutable.HashMap[String,mutable.HashMap[String,Double]]())
    for((act,timeMap)<-m2){
      val m1TimeMap = getStatMap(map1,act)
      timeMap.foreach{case (time,count)=>
        m1TimeMap.put(time,count)
      }
    }
    map1
  }

  override def copy(extra: ParamMap): UserActionChainModel = {
    copyValues(new UserActionChainModel(uid).setParent(parent), extra)
  }
}
object test{
  def main(args: Array[String]): Unit = {
    val t = "1234.587512"
    def getTime(t:String,ts:String,defaultTime:Double=(-1.0)): String ={
      val time = {
        if (Utils.isDigital(t)) t
        else if (Utils.isDigital(ts)) ts
        else defaultTime.toLong.toString
      }
      if(time.contains("."))
        time.split("\\.")(0)
      else
        time
    }
    println(Utils.isDigital(t))
    println(getTime(t,t,0.0))
  }
}