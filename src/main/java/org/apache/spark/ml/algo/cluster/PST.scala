package org.apache.spark.ml.algo.cluster

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer
/**
 * @author linsu
 *         2020/5/4 youth day, second algorithm
 */
class PST(val vocabularySize:Long,val depth:Int=5) extends Serializable{

  val nextActionsCount = new Array[Double](vocabularySize.toInt)
  val preActionsNode = new Array[PST](vocabularySize.toInt)
  def setCount(path: ListBuffer[Int],finalAction:Int):Unit = {
    if(path.length==0)
      nextActionsCount(finalAction)+=1
    else{
      val action = path.remove(0)
      if(preActionsNode(action)==null)
        preActionsNode(action) = new PST(this.vocabularySize,this.depth)
      preActionsNode(action).setCount(path,finalAction)
    }
  }

  def merge(other:PST):PST={
    for(k<-0.until(nextActionsCount.size)){
      nextActionsCount(k) = nextActionsCount(k) + other.nextActionsCount(k)
    }
    for(((t1,t2),index)<-preActionsNode.zip(other.preActionsNode).zipWithIndex){
      if(t1==null)
        this.preActionsNode(index) = t2
      else if(t2!=null)
        t1.merge(t2)
    }
    this
  }

  def build(actIds:ListBuffer[Int]):PST = {
    // a,b,c,d,e,f
    for(i<-1.to(math.min(depth,actIds.size))){
      for(j<-i.to(actIds.size)){
        val cur = actIds.slice(j-i,j)
        //        println(cur.mkString("."))
        val last = cur.remove(cur.length-1)
        setCount(cur.reverse,last)
      }
    }
    this
  }
  def print(path:String = "root",layers:Int = 3,voc:Array[String]=null):Unit ={
    println(s"path=${path} ")
    val buf = new StringBuilder()
    nextActionsCount.zipWithIndex.foreach{case (count,index)=>
      if(count>0) {
        if(voc!=null)
          buf.append(s"${voc(index)}:${count} ")
        else
          buf.append(s"${index}:${count} ")
      }
    }
    println(buf.toString())
    if(layers==0)
      return
    preActionsNode.zipWithIndex.foreach{case(node,index)=>
      if(node!=null){
        if(voc!=null)
          node.print(s"${path}->${voc(index)}",layers-1,voc)
        else
          node.print(s"${path}->${index}",layers-1,voc)
      }
    }
  }
  def rebuildAll(minCount:Double):Boolean={
    val total = nextActionsCount.sum
    if(total<=minCount)
      return true
    for(i<-0.until(nextActionsCount.size)){
      nextActionsCount(i) = nextActionsCount(i)/total
      //      if(nextActionsCount(i)==0)
      //        nextActionsCount(i) = PST.epsilon
    }
    for(i<-0.until(preActionsNode.size)){
      if(preActionsNode(i)!=null) {
        val isNeedPruned = preActionsNode(i).rebuildAll(minCount)
        if(isNeedPruned)
          preActionsNode(i)= null
      }
    }
    false
  }
  def save(path:String,spark:SparkSession):Unit={
    val rawList = new ListBuffer[(String,Vector)]
    collectData(rawList,"root")
    val frame = spark.createDataFrame(rawList.toSeq).toDF("path","data")
    frame.show(20)
    println(s"PST tree row number is ${frame.count()}")
    frame.write.mode(SaveMode.Overwrite).parquet(path)
  }
  def collectData(data:ListBuffer[(String,Vector)],parent:String):Unit={
    data.append((parent,Vectors.dense(this.nextActionsCount)))
    preActionsNode.zipWithIndex.foreach{case(node,index)=>
      if(node!=null)
        node.collectData(data,s"${parent}->${index}")
    }
  }
  def findNode(paths:Array[String]):PST={
    if(paths.length==1)
      return this
    val childPath = paths.slice(1,paths.length)
    val nextNodeIndex = childPath(0).toInt
    if(this.preActionsNode(nextNodeIndex)==null)
      this.preActionsNode(nextNodeIndex) = new PST(this.preActionsNode.size,this.depth)
    this.preActionsNode(nextNodeIndex).findNode(childPath)
  }
  def getProb(pos:Int):Double = {
    val cur = nextActionsCount(pos)
    if(cur==0)
      PST.epsilon
    else
      cur
  }
  def getConditionStyle(path:Array[Int]):Array[Int]={
    val lastArray = Array(path.last)
    path.dropRight(0).reverse.union(lastArray)
  }
  def findCondiProb(actions: Array[Int],depth:AtomicInteger = new AtomicInteger(0)):Double = {
    var prob:Double = 0.0
    depth.incrementAndGet()
    if(actions.length==1) {
      prob = getProb(actions(0))
    }
    else if(preActionsNode(actions(0))==null) {
      //      println(s"find no ${actions(0)} on ${actions.mkString(",")}")
      prob = getProb(actions.last) //target on the last
    }
    else
      prob = preActionsNode(actions(0)).findCondiProb(actions.slice(1,actions.size),depth)
    //    println(s"prob is ${prob} on ${actions.mkString(",")}")
    prob
  }
}
object PST{
  val epsilon :Double= 1e-7
  def load(path:String,spark:SparkSession,depth:Int):PST={
    val frame = spark.read.parquet(path)
    val actionNum = frame.head().getAs[Vector](1).toArray.size
    frame.rdd.aggregate(new PST(actionNum,depth))(
      (tree:PST,row:Row)=>{
        val path = row.getString(0).split("->")
        val arr = tree.findNode(path).nextActionsCount
        val arrValue = row.getAs[Vector](1).toArray
        for(i<-0.until(actionNum))
          arr(i) = arrValue(i)
        tree
      },
      (tree1:PST,tree2:PST)=>{
        tree1.merge(tree2)
      }
    )
  }
}