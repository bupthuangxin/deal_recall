package com.sankuai.meituan.utils

/**
 * #Author:      huangxin13.
 * #Date:        17/7/12.
 * #Email:       huangxin13@meituan.com.
 * #Description:
 */

object SqlProvider {

//  def returnUuidTradeSqlStr(date: String): String ={
//    """
//      |SELECT
//      |*
//      |FROM al_catering.app_mt_uuid_trade_deal_history
//      |WHERE partition_date = '""".stripMargin + date + "'"
//
//  }

  def returnUserIdTradeSqlStr(date:String):String={
    """
      |SELECT
      |*
      |FROM al_catering.app_mt_user_trade_deal_history
      |WHERE partition_date = '""".stripMargin + date + "'"
  }

  def deleteUserIdTradeSqlStr(date: String): String = {
    """ALTER TABLE al_catering.app_mt_uuid_trade_user_deal_recommend
      |DROP IF EXISTS PARTITION(partition_date = '""".stripMargin + date + "')"
  }

  def insertUserIdTradeSqlStr(date: String): String = {
    """INSERT INTO al_catering.app_mt_uuid_trade_user_deal_recommend
      |PARTITION(partition_date = '""".stripMargin + date + """')
      |SELECT * FROM app_mt_uuid_trade_user_deal_recommend""".stripMargin
  }


  def returnUserIdClickSqlStr(date:String):String={
    """
      |SELECT
      |*
      |FROM al_catering.app_mt_user_click_deal_history
      |WHERE partition_date = '""".stripMargin + date + "'"
  }

  def insertUserIdClickSqlStr(date: String): String = {
    """INSERT INTO al_catering.app_mt_uuid_click_user_deal_recommen
      |PARTITION(partition_date = '""".stripMargin + date + """')
      |SELECT * FROM app_mt_uuid_click_user_deal_recommen""".stripMargin
  }

  def returnUserIdConsumeSqlStr(date:String):String={
    """
      |SELECT
      |*
      |FROM al_catering.app_mt_userid_consume_deal_history
      |WHERE partition_date = '""".stripMargin + date + "'"
  }

  def insertUserIdConsumeSqlStr(date: String): String = {
    """INSERT INTO al_catering.app_mt_uuid_consume_user_deal_recommend
      |PARTITION(partition_date = '""".stripMargin + date + """')
      |SELECT * FROM app_mt_uuid_consume_user_deal_recommend""".stripMargin
  }

  def returnUserIdVisitSqlStr(date:String):String={
    """
      |SELECT
      |*
      |FROM al_catering.app_mt_user_visit_deal_history
      |WHERE partition_date = '""".stripMargin + date + "'"
  }

  def insertUserIdVisitSqlStr(date: String): String = {
    """INSERT INTO al_catering.app_mt_uuid_visit_user_deal_recommend
      |PARTITION(partition_date = '""".stripMargin + date + """')
      |SELECT * FROM app_mt_uuid_visit_user_deal_recommend""".stripMargin
  }


//  def returnUuid2UseridSqlStr():String={
//    """
//    |SELECT
//    |*
//    |from al_catering.app_mt_uuid_to_userid
//    """.stripMargin
//  }
}
