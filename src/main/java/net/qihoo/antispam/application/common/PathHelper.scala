package net.qihoo.antispam.application.common

/**
  * @author linsu
  * @create 2019/12/10 18:48
  */
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.Path


object PathHelper {

  def mkPath(p:Array[String]):String={
    if(p.length<2)
      throw  new Exception("invalid, path array must larger than 1")
    var path = new Path(p(0),p(1))
    for(i<-2 until p.length){
      path = new Path(path,p(i))
    }
    path.toString
  }

  def getPath(root:String,others:String*) :String={
    var path = new Path(root)
    others.foreach{ p=>
      path = new Path(path,p)
    }
    path.toString
  }
  def createAllFilePath(arr:Array[String]):String={
    if(arr.length != 4)
      throw  new Exception("invalid, path array must equal 4")
    val fm = new SimpleDateFormat("yyyyMMdd")
    var dirPath = new Path(arr(0),arr(1))
    val oneDayTime = fm.parse(arr(2)).getTime
    val sb = new StringBuffer()
    //86400000 一天的时间差,精确到秒
    for (i <- 0 until arr(3).toInt) {
      sb.append("/*,")
      //数据类型转换成Long
      sb.append(new Path(dirPath,fm.format(oneDayTime + i * 86400000l)))
    }
    val allFilePath = sb.toString.substring(3) + "/*"
    allFilePath
  }

  def main(args: Array[String]): Unit = {
    println(mkPath(Array("a","b")))
    println(mkPath(Array("a","b","c","d")))
  }


}
