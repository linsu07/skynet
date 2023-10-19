package org.apache.spark.ml.common

/**
  * @author linsu
  * @create 2019/12/10 18:48
  */
/*
println(WeekDay(0)) // 输出 meeting

println(WeekDay.Mon)      //直接取枚举值 meeting

println(WeekDay.Mon.id) //取枚举值所在序号 0

println(WeekDay.maxId) //枚举值的个数 7

println(WeekDay.withName("meeting")) //通过字符串获取枚举（这里是不需要反射的） meeting
 */
object CheckStatus extends Enumeration {

  // 缺乏数据 ，无法判断
  val unk = Value(0,"unknown")
  // 策略代码bug，出了异常
  val err = Value(1,"error")
  // 策略豁免
  val remit = Value(2,"remit")
  // 真值
  val real = Value(3,"real")
  //疑似作弊，后面跟个概率值
  val  anomaly = Value(4, "anomaly")
  //作弊
  val spam = Value(5,"spam")
}
