package com.sankuai.meituan.recall

/**
 * Created by huangxin on 17/7/31.
 */

import com.alibaba.fastjson.JSON
import com.sankuai.meituan.reader.DataReader
import com.sankuai.meituan.utils.{SqlProvider, DateProvider}
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext}
import Array._
import scala.collection.mutable.ArrayBuffer

//, SparkSession}
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.sql.hive.HiveContext


class Graphx(hiveContext:HiveContext ,
             sparkContext:SparkContext) {

  def prepareData(city_id:Long):RDD[(Long,(Int,Int,Int))]={
    val reader = new DataReader(hiveContext,sparkContext)

    val data = reader
      .prepareUserIdTrade()
      .filter(s=>s._1._1==city_id)
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
    data
  }

  def returnUserDealRdd(data:RDD[(Long,(Int,Int,Int))]): RDD[(Int,Int)] = {

    val user = data
      .map(s => s._2._1)
      .distinct()
      .collect()
      .zipWithIndex
    val deal = data
      .map(s => s._2._2)
      .distinct()
      .collect()
      .zip(Stream from user.length)
    val userDeal = concat(user, deal)
    val userDealRdd = sparkContext.parallelize(userDeal)
      .map(s => s._1 -> s._2) //userInt/dealInt->indexInt
      .cache()
    userDealRdd.take(10).foreach(println(_))
    userDealRdd
  }

  def returnIndex(data:RDD[(Long,(Int,Int,Int))]
                  ,userDealRdd:RDD[(Int,Int)]):RDD[(Int,Int,Int,Int,Int)]= {

    val totalData = data
      .map(s => s._2._1 ->(s._2._2, s._2._3)) //userInt->(itemInt,freqInt)
      .leftOuterJoin(userDealRdd)
      .map(s => s._2._1._1 ->(s._1, s._2._2.get, s._2._1._2)) //itemInt->(userInt,indexInt,freqInt))
      .leftOuterJoin(userDealRdd)
      .map(s => (s._1, s._2._2.get, s._2._1._1, s._2._1._2, s._2._1._3)) //(itemInt,indexInt,userInt,indexInt,freqInt)
    totalData.take(10).foreach(println(_))
    totalData
  }
  def returnVertexArray(userDealRdd: RDD[(Int,Int)]):RDD[(Long,(String,Int))]={

    val vertexRDD = userDealRdd
      .map(s => (s._2.toLong,(s._1.toString,0)))
    vertexRDD.take(10).foreach(println(_))
    vertexRDD
  }

  def returnEdgeArray(totalData:RDD[(Int,Int,Int,Int,Int)]):RDD[Edge[Int]] ={

    val edgeRDD = totalData
      .map(s => Edge(s._4.toLong,s._2.toLong,s._5))
    edgeRDD.take(10).foreach(println(_))
    edgeRDD
  }

  def getUserCount(data:RDD[(Long,(Int,Int,Int))]): Long={
    data.map(s=>s._2._1).distinct().count()
  }

  def getDealCount(data:RDD[(Long,(Int,Int,Int))]): Long={
    data.map(s=>s._2._2).distinct().count()
  }


//  def writeToHive(result: RDD[(Long,String)]) = {
//
//    import hiveContext.implicits._
//    val df = result.toDF("mt_user_id","recommend_deal_list")
//    val date = DateProvider.getYesterday
//    df.createOrReplaceTempView("app_mt_uuid_consume_user_deal_recommend")
//    hiveContext.sql(SqlProvider.insertUserIdConsumeSqlStr("2017-07-30"))
//    println("succeed to write hive table")
//    val end = System.currentTimeMillis()
//    val take = (end - start) * 1.0 / 1000
//    println(s"writeToHive = $take s")
//    println("_______________________")
//  }


}


object Graphx {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("graphx")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    val graphx = new Graphx(hiveContext,sc)
    val prepareData = graphx.prepareData(1)//cityLong->(userInt,itemInt,freqInt)
    val userDeal = graphx.returnUserDealRdd(prepareData)//userInt/dealInt->indexInt
    val index = graphx.returnIndex(prepareData,userDeal)//(itemInt,indexInt,userInt,indexInt,freqInt)
    val vertexRDD = graphx.returnVertexArray(userDeal)
    val edgeRDD = graphx.returnEdgeArray(index)

    val rowInx = graphx.getUserCount(prepareData)
    println(rowInx)
    val colInx = graphx.getDealCount(prepareData)
    println(colInx)



    //构造图Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    println("graph success")

    case class V(name: String, w: Int, inDeg: Int, outDeg: Int, Deg: Int)//点String，点int，入度，出度，总
    val initialVectorGraph: Graph[V, Int] = graph.mapVertices { case (id, (name, w)) => V(name, w, 0, 0, 0)}//vertexId,(String,int)
    val VectorGraph = initialVectorGraph.outerJoinVertices(initialVectorGraph.inDegrees) {
      case (id, u, inDegOpt) => V(u.name, u.w, inDegOpt.getOrElse(0), u.outDeg, inDegOpt.getOrElse(0))
    }.outerJoinVertices(initialVectorGraph.outDegrees) {
      case (id, u, outDegOpt) => V(u.name, u.w, u.inDeg, outDegOpt.getOrElse(0), u.Deg+outDegOpt.getOrElse(0))
    }//Graph[V, Int]


    val alpha = 0.8
    val matrix_matrixentry_src: RDD[MatrixEntry] = VectorGraph.triplets.map(e => MatrixEntry(e.srcId,e.dstId,-alpha/(e.dstAttr.Deg+1)))
    val matrix_matrixentry_dst: RDD[MatrixEntry] = VectorGraph.triplets.map(e => MatrixEntry(e.dstId,e.srcId,-alpha/(e.srcAttr.Deg+1)))
    val matrix_matrixentry_dia_src: RDD[MatrixEntry] = VectorGraph.vertices.map(e => MatrixEntry(e._1,e._1,1.00000000001-alpha/(e._2.Deg+1)))
    val matrix_matrixentry: RDD[MatrixEntry] = matrix_matrixentry_src.union(matrix_matrixentry_dst).union(matrix_matrixentry_dia_src)
    val coordMat: CoordinateMatrix = new CoordinateMatrix(matrix_matrixentry)

    val matA: BlockMatrix = coordMat.toBlockMatrix(10000,10000).cache()



//    VectorGraph.vertices.collect.foreach(e => println(1+alpha/(e._2.Deg+1-alpha)))
    val matrix_matrixentry_dia_init: RDD[MatrixEntry] = VectorGraph.vertices.map(e => MatrixEntry(e._1,e._1,1.0000000001+alpha/(e._2.Deg+1-alpha)))
    val coordMat_init: CoordinateMatrix = new CoordinateMatrix(matrix_matrixentry_dia_init)
    var mat_init: BlockMatrix = coordMat_init.toBlockMatrix(10000,10000).cache()



    val matrix_matrixentry_dia_eye: RDD[MatrixEntry] = VectorGraph.vertices.map(e => MatrixEntry(e._1,e._1,2))
    val coordMat_eye: CoordinateMatrix = new CoordinateMatrix(matrix_matrixentry_dia_eye)
    val mat_eye: BlockMatrix = coordMat_eye.toBlockMatrix(10000,10000).cache()


    val matrix_matrixentry_dia_eye_sub: RDD[MatrixEntry] = VectorGraph.vertices.map(e => MatrixEntry(e._1,e._1,-1.000000000001))
    val coordMat_eye_sub: CoordinateMatrix = new CoordinateMatrix(matrix_matrixentry_dia_eye_sub)
    val mat_eye_sub: BlockMatrix = coordMat_eye_sub.toBlockMatrix(10000,10000).cache()

    for (a <- 1 to 2){
      //println(a)
      val mat_temp1: BlockMatrix = matA.multiply(mat_init)
      //println("end1")
      //println(mat_temp1.numCols()+" "+mat_temp1.numRows())
      val mat_temp3: BlockMatrix = mat_temp1.multiply(mat_eye_sub)
      //println("end2")
      //println(mat_temp3.numCols()+" "+mat_temp3.numRows())
      val mat_temp2: BlockMatrix = mat_eye.add(mat_temp3)
      //println("end3")
      //println(mat_temp2.numCols()+" "+mat_temp2.numRows())
      mat_init = mat_init.multiply(mat_temp2)
      //println("end4")
      //println(mat_init.numCols()+" "+mat_init.numRows())
    }
//

    val userDeal2 = userDeal.map(s=>s._2.toLong->s._1.toLong)//index->id

    val C = mat_init
      .toCoordinateMatrix()
      .entries
      .filter(s => s.i < rowInx)
      .filter(s => s.j > rowInx)
      .map(s => s.i -> (s.j, s.value))//userindex->(dealindex,value)
      .leftOuterJoin(userDeal2)//userindex->((dealindex,value),userid)
      .map(s => s._2._1._1->(s._2._2.get,s._2._1._2))//dealindex->(userid,value)
      .leftOuterJoin(userDeal2)//dealindex->((userid,value),dealid)
      .map(s => s._2._1._1->(s._2._2.get,s._2._1._2))//userid,dealid,value
      //.filter(s=>s._2._2.isNaN)

      C.take(10).foreach(println(_))
      println(C.count())

    val result = C
      .groupByKey()
      .filter(s => s._2.nonEmpty)
      .map(s => {
        var ind = 0
        var topN = new Array[(Long,Double)](Math.min(10,s._2.size))
        s._2.foreach(t => {
          if(ind < 10){
            topN.update(ind,(t._1,t._2))
            ind += 1
          }
          else{
            topN = topN.sorted(Ordering.by[(Long, Double), Double] {case (dealid, value) => value})
            if(t._2>topN.apply(0)._2) topN.update(0,(t._1,t._2))
          }
        })
        (s._1,topN.length)
      })
      .take(10)
      .foreach(println(_))



    sc.stop()
  }
}