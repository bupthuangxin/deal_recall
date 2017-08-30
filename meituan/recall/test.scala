package com.sankuai.meituan.recall

import com.alibaba.fastjson.JSON

import scala.collection.mutable.ArrayBuffer

/**
 * Created by huangxin on 17/7/18.
 */

object test {
  def main(args: Array[String]): Unit = {
    val string = "[{\"deal_id\":39582130,\"frequency\":1,\"timestamp\":[1500637717131]}]"
    val arrayBuffer = new ArrayBuffer[(String,String)]()
    val json = JSON.parseArray(string)
    for(i:Int <-0 until json.size()){
      val deal_id = JSON.parseObject(json.getString(i)).getString("deal_id")
      val frequency = JSON.parseObject(json.getString(i)).getString("frequency")
      arrayBuffer += ((deal_id,frequency))
    }
    val a = arrayBuffer.toList
    println(a(0))
  }
}
