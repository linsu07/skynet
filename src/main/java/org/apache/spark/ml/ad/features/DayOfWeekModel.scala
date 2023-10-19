package org.apache.spark.ml.ad.features

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.ml.Model
import org.apache.spark.ml.common.{AntiSpamAlgorithm, AntiSpamModel, Utils}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.date_format

import org.apache.spark.sql.types.StructType

class DayOfWeekModel (override val uid:String)extends AntiSpamModel[DayOfWeekModel] {
  override def copy(extra: ParamMap): DayOfWeekModel = ???
  override def transform(in: Dataset[_]): DataFrame = {
  val addday = udf{
  (ts:String)=>
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -5)
    var yesterday = dateFormat.format(cal.getTime())
    val date = if (Utils.isDigital(ts)) dateFormat.format(new Date(ts.toLong)) else  yesterday
    date
}

  var searchData = in.withColumn("date",addday(col(${inputCols}(0))))
  searchData = searchData.withColumn(${outputCols}(0), date_format(col("date"), "E"))
  searchData.drop("date")
}
  override def transformSchema(schema: StructType): StructType = {
  schema
}
}
