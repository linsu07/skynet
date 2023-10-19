package org.apache.spark.ml.algo

/**
  * @author linsu
  *         2020/5/4 youth day, sencond algorithm
  */

import com.typesafe.config.Config
import org.apache.commons.math3.fitting.leastsquares.ParameterValidator
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.algo.cluster.PST
import org.apache.spark.ml.common.AntiSpamAlgorithm
import org.apache.spark.ml.param.{BooleanParam, DoubleArrayParam, IntParam, ParamMap, Params, StringArrayParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.linalg.{Vector, Vectors}
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
trait MarkovChainParam extends Params{
  val minCount = new IntParam(this,"minCount","prune if less than this number")
  val vocSize = new IntParam(this,"vocSize",doc = "")

  val treeDepth = new IntParam(this, "treeDepth","树的深度")
  val  prioriProbability = new DoubleArrayParam(this,"prioriProbability","先验概率")
  val isStandard = new BooleanParam(this,"isStandard","是否是标准的")
  val dimemsions = new StringArrayParam(this,"dimemsions","")
  setDefault(minCount->20,treeDepth->8,vocSize->10,prioriProbability->Array(0.5,0.5)
    ,isStandard->false,dimemsions->Array("Srcg"))
  def isaStandard()={
    ${isStandard}
  }
  def getTreeDepth()={
    ${treeDepth}
  }
}

class MarkovChain(override val uid:String) extends Estimator[MarkovChainModel] with AntiSpamAlgorithm with MarkovChainParam {
  def this() = this(Identifiable.randomUID("MarkovChain"))
  val signItem = "guid_search_num"
  val signItem_click = "guid_click_num"
  var voc:Array[String] = null
  override def setConfig(c: Config): this.type = {
    super.setConfig(c)
    if(c.hasPath("minCount"))
      set(minCount->c.getInt("minCount"))
    if(c.hasPath("treeDepth"))
      set(treeDepth->c.getInt("treeDepth"))
    if(c.hasPath("vocSize"))
      set(vocSize->c.getInt("vocSize"))
    if(c.hasPath("prioriProbability"))
      set(prioriProbability->c.getDoubleList("prioriProbability").asScala.toArray.map(_.toDouble))
    if(c.hasPath("isStandard"))
      set(isStandard->c.getBoolean("isStandard"))
    if(c.hasPath("dimemsions"))
      set(dimemsions->c.getStringList("dimemsions").asScala.toArray)
    this
  }
  override def fit(dataset: Dataset[_]): MarkovChainModel = {
    if(${isStandard}) {
      standardFit(dataset)
    }else{
      testFit(dataset)
    }
  }
  def testFit(dataset: Dataset[_]): MarkovChainModel = {
    val treeMap = dataset.select(${dimemsions}.union(Array(${inputCols}(0))).map(col(_)):_*).rdd
      .keyBy(row=>AntiSpamAlgorithm.getStatKey(${dimemsions},row))
      .aggregateByKey(new PST(${vocSize},depth=${treeDepth}))(
        (tree:PST,row:Row)=>{
          val actions = row.getAs[Seq[Int]](${inputCols}(0))
          val buf = new ListBuffer[Int]()
          buf.appendAll(actions)
          tree.build(buf)
        },
        (tree1:PST,tree2:PST)=>{
          tree1.merge(tree2)
        }
      ).filter(_._2.nextActionsCount.sum>${statLowerLimits}).map{ case(key,t)=>
      t.rebuildAll(${minCount})
      (key,t)
    }.collect().toMap
    val m = copyValues(new MarkovChainModel(uid))
    m.treeMap = treeMap
    m
  }
  def standardFit(dataset: Dataset[_]): MarkovChainModel = {
    val tree = dataset.select(${inputCols}(0)).rdd.aggregate(new PST(${vocSize},depth=${treeDepth}))(
      (tree:PST,row:Row)=>{
        val actions = row.getAs[Seq[Int]](${inputCols}(0))
        val buf = new ListBuffer[Int]()
        buf.appendAll(actions)
        tree.build(buf)
      },
      (tree1:PST,tree2:PST)=>{
        tree1.merge(tree2)
      }
    )
    tree.rebuildAll(${minCount})
    val m = copyValues(new MarkovChainModel(uid))
    m.tree = tree
    set(this.prioriProbability->tree.nextActionsCount)
    println(tree.nextActionsCount.mkString(","))
    m
  }

  override def copy(extra: ParamMap): Estimator[MarkovChainModel] =  {
    defaultCopy(extra)
  }
  override def transformSchema(schema: StructType): StructType = {
    super.checkSchema(schema)
  }
}

object MarkovChain{
  def main(args: Array[String]): Unit = {
    //    val t = new StatTree(3)
    //    print(t)
    //    val actIds = new ListBuffer[Int]()
    //    //1000100110
    //    actIds.appendAll(Array(1,0,0,0,1,0,0,1,1,0))
    //    println(s"sequence : ${actIds.mkString(",")}")
    //    val root = new PST(actIds.size+10)
    //    val root1 = new PST(actIds.size+10)
    //    root.build(actIds)
    //    root1.build(actIds)
    //    root.merge(root1)
    //    root.rebuildAll(2)
    //    root.print()
    //    val spark = SparkSession.builder().appName("myapp").master("local").getOrCreate()
    //    root.save("/tmp/PSTexample/",spark)
    //    val tree = PST.load("/tmp/PSTexample/",spark)
    //    tree.print()
    //    spark.close()

    //    println(math.log(PST.epsilon))
  }
}