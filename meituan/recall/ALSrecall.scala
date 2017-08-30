package com.sankuai.meituan.recall

import com.alibaba.fastjson.JSON
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import com.sankuai.meituan.utils.{DateProvider, SqlProvider}
import com.sankuai.meituan.reader.DataReader
import org.apache.spark.HashPartitioner
import scala.collection.mutable
import org.jblas.DoubleMatrix
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType};

import scala.collection.mutable.ArrayBuffer


/**
 * #Author:      huangxin13.
 * #Date:        17/7/12.
 * #Email:       huangxin13@meituan.com.
 * #Description: user-deal协同过滤
 */

class ALSrecall(hiveContext:HiveContext ,
                sparkContext:SparkContext) {


  def prepareUuidTrade(id:Long): RDD[(Long,(Int,Int,Int))]={
    val reader = new DataReader(hiveContext,sparkContext)

    val uuidTrade = reader
      .prepareUserIdTrade()
      .filter(s=>s._1._1==id)
      .flatMapValues(s => {
        val arrayBuffer = new ArrayBuffer[(String,String)]()
        val json = JSON.parseArray(s)
        for(i: Int <- 0 until json.size()){
          val jsonStr = JSON.parseObject(json.getString(i))
          val deal_id = jsonStr.getString("deal_id")
          val frequency = jsonStr.getString("frequency")
          arrayBuffer += ((deal_id,frequency))
        }
        arrayBuffer.toList
      })
      .map(s => s._1._1.toLong -> (s._1._2.toInt,s._2._1.toInt,s._2._2.toInt))//cityLong->(userInt,itemInt,freqInt)
      .cache()
    //uuidTrade.take(10).foreach(println(_))
    uuidTrade
  }

  def getDealCount(data:RDD[(Long,(Int,Int,Int))]):Int={
    data.map(s=>s._2._2).distinct().count().toInt
  }



  //数据准备
  def prepareDataSet(uuidTrade: RDD[(Long,(Int,Int,Int))]): RDD[Rating] = {
    val data = uuidTrade
      .map(s => Rating(s._2._1.toInt,s._2._2.toInt,s._2._3.toDouble))
      .cache()
    data
  }

  //训练模型
  def trainModel(iter: Int,
                 rank: Int,
                 lambda: Double,
                 blocks: Int,
                 dataSet: RDD[Rating]): MatrixFactorizationModel = {

    val start = System.currentTimeMillis()
    println(start)
    val als = ALS.train(dataSet,rank = rank,iterations = iter,lambda = lambda,blocks = blocks)
    val end = System.currentTimeMillis()
    val take = (end - start) * 1.0 / 1000
    println(s"training time = $take s")
    als
  }

  //选择topN
  def recommend(model:MatrixFactorizationModel,topSize:Int,dealCount:Int): RDD[(Int,String)]={

    val start = System.currentTimeMillis()

    val productFeaturesBroadCast = sparkContext.broadcast(model.productFeatures.collect())
    val userFeature = model.userFeatures
      .map(s => {
        var ind = 0
        var topN = new Array[(Int,Double)](Math.min(topSize,dealCount))
        val a = new DoubleMatrix(s._2)
        for(pfb <- productFeaturesBroadCast.value){
          val b = new DoubleMatrix(pfb._2)
          val c = a.dot(b)
          if(ind<topSize){
            topN.update(ind,(pfb._1,c))
            ind +=1
          }
          else{
            topN = topN.sorted(Ordering.by[(Int, Double), Double]{case(id,sim)=>sim})
            if(c > topN.apply(0)._2) topN.update(0,(pfb._1,c))
          }
        }
        (s._1,"{"+topN.map(s=>List(s._1.toString,s._2.toString).mkString(":")).toList.mkString(",")+"}")
      })
      .filter(s=>s._2.nonEmpty)

    //userFeature.take(10).foreach(println(_))

    val end = System.currentTimeMillis()
    val take = (end - start) * 1.0 / 1000
    println(s"topN time = $take s")
    userFeature
  }





  def writeToHive(result: RDD[(Int,String)]) = {

  val start = System.currentTimeMillis()

  // 用来把RDD隐式转换为DF(DataFrame)
    import hiveContext.implicits._
    val df = result.toDF("mt_user_id","recommend_deal_list")
    println("toDF")
    val date = DateProvider.getYesterday
    df.createOrReplaceTempView("app_mt_uuid_trade_user_deal_recommend")
    println("temp")
    hiveContext.sql(SqlProvider.deleteUserIdTradeSqlStr(date))
    hiveContext.sql(SqlProvider.insertUserIdTradeSqlStr(date))
  //hiveContext.sql(SqlProvider.insertUserIdConsumeSqlStr("2017-07-30"))
  println("succeed to write hive table")
    val end = System.currentTimeMillis()
    val take = (end - start) * 1.0 / 1000
    println(s"writeToHive = $take s")
    println("_______________________")
  }



}

object ALSrecall{

  def main (args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("ALSrecall")
    sparkConf.set("spark.driver.maxResultSize","8G")
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sparkContext)

    val als = new ALSrecall(hiveContext, sparkContext)

    //hiveContext.sql("select distinct city_id from")

    var city_id_array = new ArrayBuffer[Long]()
    city_id_array += 30


    for(i <- city_id_array){
      println(i)
      val uuidTrade = als.prepareUuidTrade(i)
      val data = als.prepareDataSet(uuidTrade)
      val model = als.trainModel(10, 16, 0.01, -1, data)
      val recommend = als.recommend(model,100,als.getDealCount(uuidTrade))
      als.writeToHive(recommend)
    }
    println("all over")

  }
}




//  def computeRmse(model:MatrixFactorizationModel,
//                  data:RDD[Rating]): Double = {
//    val userProduct = data.map{
//      case Rating(user, product, rate) => (user, product)
//    }
//
//    val predictions = model.predict(userProduct).map{
//      case Rating(user,product,rate) => ((user,product),rate)
//    }
//
//    val ratesAndPreds = data.map{
//      case Rating(user,product,rate) => ((user,product),rate)
//    }
//    .join(predictions)
//
//    val rmse = math.sqrt(ratesAndPreds.map{
//      case((user,product),(actual, predicted)) => math.pow(actual-predicted, 2)
//    }.mean())
//
//    rmse
//  }