package org.apache.spark.ml.common

object RuleType extends Enumeration  {
  val commercial = Value(0,"CommercialAntiSpam")
  val guid = Value(1,"BusyUser")
  val distribute = Value(2,"DistributeDisorder")
  val invalid = Value(3,"InvalidBehavior")
  def main(args: Array[String]): Unit = {
    val a = RuleType.withName("CommercialAntiSpam")
    print(a.toString)
  }
}
