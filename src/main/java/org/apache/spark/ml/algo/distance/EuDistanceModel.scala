package org.apache.spark.ml.algo.distance

/**
 * @author linsu 2020/08/05
 */
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.ml.common.AntiSpamModel
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable}
import org.apache.spark.sql.{DataFrame, Dataset, Row}




class EuDistanceModel(override val uid: String) extends  AntiSpamModel[EuDistanceModel] with EuDistanceParam
  with  DefaultParamsWritable {

  override def setConfig(c: Config): EuDistanceModel.this.type = {
    setSpamThreshold(c.getDouble("spamThreshold"))
    setSigmaOffset(c.getInt("sigmaOffset"))
    super.setConfig(c)

  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    //val inversedCovarience = getConv()
    var df = dataset.asInstanceOf[DataFrame]
//    val calDistance = udf {
//      (row: Row) =>
//        val features = row.toSeq.map(_.toString.toDouble).toArray
//        val dealfeatures = features.zip(mean).map { case (cur, best) => cur - best }
//        val x = new DenseMatrix(1, ${covSize}, dealfeatures)
//        val xTranspose = x.transpose
//        var ret = x.multiply(inversedCovarience)
//        ret = ret.multiply(xTranspose)
//        math.sqrt(ret.values(0))
//    }
//    val calDistance1 = udf {
//      (row: Row) =>
//        //计算马氏距离
//        val features = row.toSeq.map(_.toString.toDouble).toArray
//        val dealfeatures: Array[Double] = features.zip(mean).map { case (cur, best) => cur - best }
//        //计算只根据最大值的得到的马氏距离
//        //最大值下标
//        //val maxIndex = features.zipWithIndex.maxBy(_._1)._2
//        val maxIndex = dealfeatures.map(math.abs(_)).zipWithIndex.maxBy(_._1)._2
//        //替换除最大值外的差值
//        //零元素数组
//        val zerofeatures = new Array[Double](dealfeatures.length)
//        zerofeatures(maxIndex) = dealfeatures(maxIndex)
//        val y = new DenseMatrix(1, ${covSize}, zerofeatures)
//        val yTranspose = y.transpose
//        var rety = y.multiply(inversedCovarience)
//        rety = rety.multiply(yTranspose)
//        math.sqrt(rety.values(0))
//
//    }
    val calDistance = udf {
      (row: Row) =>
        val features = row.toSeq.map(_.toString.toDouble).toArray
        //val dealFeatures = features.zip(mean).map { case (cur, best) => cur - best }
        val x = new DenseMatrix(1, features.length, features)
        val xTranspose = x.transpose
        val ret = x.multiply(xTranspose)
        var eudistance = math.sqrt(ret.values(0))
        //处理单特征异常的特殊情况
        //计算次大特征的偏移
        if (eudistance > getSpamThreshold()){
          val sortFeatures = features.zipWithIndex.sortWith(_._1 > _._1)
          val secondFeatureIndex = sortFeatures(1)._2
          //val secondOffset = math.abs(dealFeatures(secondFeatureIndex))/sigma(secondFeatureIndex)
          if(features(secondFeatureIndex) < getSigmaOffset()){
            eudistance = getSpamThreshold() - 0.1
          }
        }
        eudistance
    }
//    val calDistance3 = udf {
//      (row: Row) =>
//        //计算与均值的距离
//        val features = row.toSeq.map(_.toString.toDouble).toArray
//        val dealfeatures: Array[Double] = features.zip(mean).map { case (cur, best) => cur - best }
//        //最大值下标
//        val maxIndex = dealfeatures.map(math.abs(_)).zipWithIndex.maxBy(_._1)._2
//        //零元素数组
//        val zerofeatures = new Array[Double](dealfeatures.length)
//        //替换除最大值外的差值
//        zerofeatures(maxIndex) = dealfeatures(maxIndex)
//        val y = new DenseMatrix(1, ${covSize}, zerofeatures)
//        val yTranspose = y.transpose
//        val rety = y.multiply(yTranspose)
//        math.sqrt(rety.values(0))
//
//    }
    df = df.withColumn(${outputCols}(0),calDistance(struct(${inputCols}.map(col): _*)))

    df
  }

  override def copy(extra: ParamMap): EuDistanceModel = {this}
}
object EuDistanceModel extends DefaultParamsReadable[EuDistanceModel]
