package org.apache.spark.ml.ad

import org.apache.spark.ml.ad.InfoFromDetailArryJsonHelper.getFromRow
import org.apache.spark.sql.Row
import org.apache.spark.ml.common.Utils.isDigital
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @Author zhubaowen
  * @create 2020/7/10 11:33
  */

trait expression{
  val colname = "feature"
  def compare(valueMap:mutable.HashMap[String,ListBuffer[Any]]):ListBuffer[Int]
  def compareFullList(valueMap:mutable.HashMap[String,ListBuffer[Any]]):ListBuffer[Boolean]
  def getValue(row:Row,valueMap:mutable.HashMap[String,ListBuffer[Any]]):Boolean ={
    var value = new ListBuffer[Any]
    if (colname.contains("."))
      value = getFromRow(row, colname: String)
    else
      value.append(row.getAs[Int](colname).toString)
    valueMap.put(colname,value)
    value.isEmpty
  }
}
//class SimpleNumberCompare(override val colname:String,opr:String,objValue:Int) extends Serializable with expression{
//  override def compare(valueMap:mutable.HashMap[String,ListBuffer[Any]]): ListBuffer[Int] = {
//    var res = new ListBuffer[Int]
//    var res1 = new ListBuffer[Int]
//    //    var Flag = false
//    //    if (valueMap.get(colname).get.contains("2")){
//    //      println("1111",valueMap.get(colname).get.toList)
//    //      Flag = true
//    //    }
//    valueMap.get(colname).get.zipWithIndex.foreach(item =>{
//      println("item is ",item)
//      val value = item._1.toString.toInt
//      val ret = opr match {
//        case ">" => value > objValue
//        case "<" => value < objValue
//        case ">=" => value >= objValue
//        case "<=" => value <= objValue
//        case "==" => value == objValue
//      }
//      //      if (Flag)
//      //        println("ret",ret)
//      ret match {
//        case false => res1.append(item._2)
//        case true => res.append(item._2)
//      }
//    })
//    //    if (Flag)
//    //      println("22222222",res1.toList,res.toList)
//    res
//  }
//
//  override def compareFullList(valueMap:mutable.HashMap[String,ListBuffer[Any]]): ListBuffer[Boolean] = {
//    var res = new ListBuffer[Boolean]
//    valueMap.get(colname).get.zipWithIndex.foreach(item =>{
//      val value = item._1.toString.toInt
//      val ret = opr match {
//        case ">" => value > objValue
//        case "<" => value < objValue
//        case ">=" => value >= objValue
//        case "<=" => value <= objValue
//        case "==" => value == objValue
//      }
//
//      ret match {
//        case false => res.append(false)
//        case true => res.append(true)
//      }
//    })
//    res
//  }
//}

class SimpleNumberCompare(override val colname:String,opr:String,objValueOrigin:String) extends Serializable with expression{
  override def compare(valueMap:mutable.HashMap[String,ListBuffer[Any]]): ListBuffer[Int] = {
    var res = new ListBuffer[Int]
    var res1 = new ListBuffer[Int]
    //    var Flag = false
    //    if (valueMap.get(colname).get.contains("2")){
    //      println("1111",valueMap.get(colname).get.toList)
    //      Flag = true
    //    }
    valueMap.get(colname).get.zipWithIndex.foreach(item =>{
      val dataFlag = isDigital(item._1.toString)
      val objectFlag = isDigital(objValueOrigin)
      var ret = false
      if(dataFlag && objectFlag){
        val value = item._1.toString.toInt
        val objValue = objValueOrigin.toInt
        ret = opr match {
          case ">" => value > objValue
          case "<" => value < objValue
          case ">=" => value >= objValue
          case "<=" => value <= objValue
          case "==" => value == objValue
          case "!=" => value != objValue
        }
      }
      else{
        val value = item._1.toString
        val objValue = objValueOrigin
        ret = opr match {
          case ">" => value > objValue
          case "<" => value < objValue
          case ">=" => value >= objValue
          case "<=" => value <= objValue
          case "==" => value == objValue
          case "!=" => value != objValue
        }
      }


      //      if (Flag)
      //        println("ret",ret)
      ret match {
        case false => res1.append(item._2)
        case true => res.append(item._2)
      }
    })
    //    if (Flag)
    //      println("22222222",res1.toList,res.toList)
    res
  }

  override def compareFullList(valueMap:mutable.HashMap[String,ListBuffer[Any]]): ListBuffer[Boolean] = {
    var res = new ListBuffer[Boolean]
    valueMap.get(colname).get.zipWithIndex.foreach(item =>{
      val dataFlag = isDigital(item._1.toString)
      val objectFlag = isDigital(objValueOrigin)
      var ret = false
      if(dataFlag && objectFlag){
        val value = item._1.toString.toDouble
        val objValue = objValueOrigin.toDouble
        ret = opr match {
          case ">" => value > objValue
          case "<" => value < objValue
          case ">=" => value >= objValue
          case "<=" => value <= objValue
          case "==" => value == objValue
          case "!=" => value != objValue
        }
      }
      else{
        val value = item._1.toString
        val objValue = if(objValueOrigin=="\'\'") "" else objValueOrigin
        ret = opr match {
          case ">" => value > objValue
          case "<" => value < objValue
          case ">=" => value >= objValue
          case "<=" => value <= objValue
          case "==" => value == objValue
          case "!=" => value != objValue
        }
      }

      ret match {
        case false => res.append(false)
        case true => res.append(true)
      }
    })
    res
  }
}

object SimpleExpression{
  //val SimpleCompareEx = """([a-z|A-Z|0-9|\-|_|\.]+)\s*(>|<|>=|<=|==)\s*([0-9|\.]+)""".r
  val SimpleCompareEx = """([a-z|A-Z|0-9|\-|_|\.]+)\s*(>|<|>=|<=|==|!=)\s*([a-z|A-Z|0-9|\-|_|\.|\u4e00-\u9fa5|\']+)""".r
  def parse(expr:String):expression = {
    expr.trim match {
      case SimpleCompareEx(colname,opr,value) => new SimpleNumberCompare(colname,opr,value.toString)
      case _=>throw new IllegalArgumentException("expr [%s] is not legal, pls check".format(expr))
    }
  }
  def main(args: Array[String]): Unit = {
    val a = ""
    val exprFlag: expression = parse(s"""a!=""""")
    println(exprFlag)


  }
}


