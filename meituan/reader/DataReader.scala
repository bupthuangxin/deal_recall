package com.sankuai.meituan.reader

import com.sankuai.meituan.utils.{DateProvider, SqlProvider}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * #Author:      huangxin13.
 * #Date:        17/7/12.
 * #Email:       huangxin13@meituan.com.
 * #Description:
 */

class DataReader(hiveContext: HiveContext,
                 sparkContext: SparkContext) {

  private val date = DateProvider.getYesterday

//  def prepareUuidTrade(): RDD[((Long,String),String)] ={
//    val peer = hiveContext
//      .sql(SqlProvider.returnUuidTradeSqlStr(date))
//      .rdd
//      .map(s => s -> !s.anyNull)
//      .filter(s => s._2)
//      .map(s => (s._1.getLong(0),s._1.getString(1)) -> s._1.getString(2))
//    peer
//  }

  def prepareUserIdTrade():RDD[((Long,Long),String)]={
    val peer = hiveContext
      .sql(SqlProvider.returnUserIdTradeSqlStr(date))
      .rdd
      .map(s => s -> !s.anyNull)
      .filter(s => s._2)
      .map(s => (s._1.getLong(0),s._1.getLong(1)) -> s._1.getString(2))
    peer
  }

  def prepareUserIdClick():RDD[((Long,Long),String)]={
    val peer = hiveContext
      .sql(SqlProvider.returnUserIdClickSqlStr(date))
      .rdd
      .map(s => s -> !s.anyNull)
      .filter(s => s._2)
      .map(s => (s._1.getLong(0),s._1.getLong(1)) -> s._1.getString(2))
    peer
  }

  def prepareUserIdConsume():RDD[((Long,String),String)]={
    val peer = hiveContext
      .sql(SqlProvider.returnUserIdConsumeSqlStr(date))
      .rdd
      .map(s => s -> !s.anyNull)
      .filter(s => s._2)
      .map(s => (s._1.getLong(0),s._1.getString(1)) -> s._1.getString(2))
    peer
  }

  def prepareUserIdVisit():RDD[((String,Long),String)]={
    val peer = hiveContext
      .sql(SqlProvider.returnUserIdVisitSqlStr(date))
      .rdd
      .map(s => s -> !s.anyNull)
      .filter(s => s._2)
      .map(s => (s._1.getString(0),s._1.getLong(1)) -> s._1.getString(2))
    peer
  }


//  def prepareUuid2Userid():RDD[(String,Long)]={
//    val peer = hiveContext
//      .sql(SqlProvider.returnUuid2UseridSqlStr())
//      .rdd
//      .map(s => s -> !s.anyNull)
//      .filter(s => s._2)
//      .map(s => s._1.getString(0)->s._1.getLong(1))
//    peer
//  }

}
