//package org.apache.spark.ml.algo
//
//import java.io.Serializable
//
//import org.apache.spark.sql.Column
//import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete}
//
//class ExtraSummarizer {
//
//  def skewnessCoeff(feature:Column):Column ={
//    new Column(AggregateExpression(agg, mode = Complete, isDistinct = false))
//  }
//  def peakednessCoeff(feature:Column):Column = {
//    new Column(AggregateExpression(agg, mode = Complete, isDistinct = false))
//  }
//
//}
//
//class SummaryAgregator{
//
//}
//
//sealed trait Metric extends Serializable
//private[stat] case object skewness extends Metric
//private[stat] case object peakedness extends Metric
//
//sealed trait ComputeMetric extends Serializable
//private[stat] case object ComputeSkewness extends ComputeMetric
//private[stat] case object ComputeSeakedness extends ComputeMetric