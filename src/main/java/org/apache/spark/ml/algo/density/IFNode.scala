package org.apache.spark.ml.algo.density

sealed abstract class IFNode extends Serializable {
}

/**
  * Data Structure for Isolation Forest Internal Node
  * @param leftChild
  * @param rightChild
  * @param featureIndex
  * @param featureValue
  */
class IFInternalNode (
    val leftChild: IFNode,
    val rightChild: IFNode,
    val featureIndex: Int,
    val featureValue: Double) extends IFNode {
  def getTotalLength() :Long = {
    getChildLength(leftChild)+getChildLength(rightChild)
  }
  def getChildLength(child:IFNode):Long = {
      if (child.isInstanceOf[IFInternalNode])
        child.asInstanceOf[IFInternalNode].getTotalLength()
      else
        child.asInstanceOf[IFLeafNode].numInstance
  }
}

class IFLeafNode (
    val numInstance: Long) extends IFNode {
}
