package org.apache.spark.ml.common

import org.apache.spark.ml.common.CheckStatus.Value

object CheckStatusPlus  extends Enumeration {

  // 缺乏数据 ，无法判断
  val unk = Value(0,"unknown")
  // 策略代码bug，出了异常
  val err = Value(1,"error")
  // 策略豁免
  val remit = Value(2,"remit")
  // 真值
  val real = Value(3,"real")
  //疑似作弊，后面跟个概率值
  val  anomalyLow = Value(4, "anomalyLow")
  val  anomalyMid = Value(5, "anomalyMid")
  val  anomalyHigh = Value(6, "anomalyHigh")
  //作弊
  val spam = Value(8,"spam")

}
