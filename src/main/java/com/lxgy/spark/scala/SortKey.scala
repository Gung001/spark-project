package com.lxgy.spark.scala

/**
  * @author Gryant
  */
class SortKey(val clickClick: Int, val orderClick: Int, val payClick: Int)
  extends Ordered[SortKey] with Serializable {

  override def compare(that: SortKey): Int = {

    if (clickClick - that.clickClick != 0) {
      clickClick - that.clickClick
    } else if (orderClick - that.orderClick != 0) {
      orderClick - that.orderClick
    } else if (payClick - that.payClick != 0) {
      payClick - that.payClick
    } else {
      0
    }
  }
}
