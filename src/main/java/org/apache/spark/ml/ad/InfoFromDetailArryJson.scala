package org.apache.spark.ml.ad

import java.util

import com.google.common.reflect.TypeToken
import com.google.gson.{Gson, JsonArray, JsonElement, JsonParser}

import scala.collection.JavaConversions._
import org.apache.spark.sql.Row

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONObject



object InfoFromDetailArryJsonHelper{
  val json = new Gson
  def getFromString(str:String)={
    var jsonArray: JsonArray =new JsonParser().parse(str).getAsJsonArray
    jsonArray
  }

  def getFromRow(row:Row,fieldname:String,flatFlag:Boolean=true)={
    val itemInfo = fieldname.split("\\.")
    val str = row.getAs[String](itemInfo.head)
    try{
      val jsonInfo = getFromString(str:String)
      getJsondata(jsonInfo,itemInfo.toList,1,itemInfo(itemInfo.size-1),flatFlag)
    }catch {
      case e:Throwable=>
        new ListBuffer[Any]()
    }
  }

  def getJsondata(sourcedata:JsonArray, item:List[String],index:Int, finalindex:String,flatFlag:Boolean=true): ListBuffer[Any] ={
    val result = new ListBuffer[Any]
    if(item(index) == finalindex){
      sourcedata.foreach(info=>{
        if(info.getAsJsonObject.get(item(index)).getClass.toString.toLowerCase().contains("array"))
            if(flatFlag)
              {
                result.appendAll(info.getAsJsonObject.get(item(index)).getAsJsonArray.map(line => line.getAsString))
              }
            else {
              result.append(List(info.getAsJsonObject.get(item(index))))
          }
        else{
          result.append(info.getAsJsonObject.get(item(index)).getAsString)
        }
      })
    }
    else{
      sourcedata.foreach(info=>{
        val index_temp = index+1
        val temp = getJsondata(info.getAsJsonObject.get(item(index)).getAsJsonArray,item,index_temp,finalindex)
        if(flatFlag)
          result.appendAll(temp)
        else
          result.append(temp)
      })
    }
    result
  }


  def main(args: Array[String]): Unit = {
    //val detail = "[{\"ts\":\"1580674224000\",\"ip\":\"58.63.190.4\",\"pn\":\"4\",\"srcg\":\"cs_huawei_4_new\",\"mod\":\"og\",\"cat\":\"\",\"pos\":\"3\",\"clicktype\":\"link\",\"screen\":\"9\",\"scr_time\":\"0\",\"official_click\":\"\",\"kids_click\":\"\",\"ipvc\":\"广东\",\"ict\":\"中山\",\"t\":\"1580674229736\"},{\"ts\":\"1580674237000\",\"ip\":\"58.63.190.4\",\"pn\":\"4\",\"srcg\":\"cs_huawei_4_new\",\"mod\":\"og\",\"cat\":\"\",\"pos\":\"10\",\"clicktype\":\"link\",\"screen\":\"11\",\"scr_time\":\"0\",\"official_click\":\"\",\"kids_click\":\"\",\"ipvc\":\"广东\",\"ict\":\"中山\",\"t\":\"1580674242742\"},{\"ts\":\"1580674220000\",\"ip\":\"58.63.190.4\",\"pn\":\"4\",\"srcg\":\"cs_huawei_4_new\",\"mod\":\"om\",\"cat\":\"mso-news\",\"pos\":\"1\",\"clicktype\":\"link\",\"screen\":\"8\",\"scr_time\":\"1\",\"official_click\":\"\",\"kids_click\":\"\",\"ipvc\":\"广东\",\"ict\":\"中山\",\"t\":\"1580674225610\"},{\"ts\":\"1580674215000\",\"ip\":\"58.63.190.4\",\"pn\":\"4\",\"srcg\":\"cs_huawei_4_new\",\"mod\":\"og\",\"cat\":\"\",\"pos\":\"2\",\"clicktype\":\"link\",\"screen\":\"8\",\"scr_time\":\"3\",\"official_click\":\"\",\"kids_click\":\"\",\"ipvc\":\"广东\",\"ict\":\"中山\",\"t\":\"1580674220995\"}]"
    val detail = "[{\"pn\":\"4\",\"srcg\":[{\"pos\":\"4\"},{\"pos\":\"2\"}]},{\"pn\":\"4\",\"srcg\":[]}]"

    val res = getFromString(detail)
    val result = getJsondata(res,List("detail","srcg","pos"),1,"pos",flatFlag = true)
    println(result)
    val result1 = getJsondata(res,List("detail","srcg","pos"),1,"pos",flatFlag = false)
    println(result1)
  }
}


