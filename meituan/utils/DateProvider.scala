package com.sankuai.meituan.utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * #Author:      huangxin13.
 * #Date:        17/7/12.
 * #Email:       huangxin13@meituan.com.
 * #Description:
 */

object DateProvider {

  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def getToday: String = {

    val cal = Calendar.getInstance()
    val date = dateFormat.format(cal.getTime)
    date
  }

  def getYesterday: String = {

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    val date = dateFormat.format(cal.getTime)
    date
  }

  def getBeforeYesterday: String = {

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -2)
    val date = dateFormat.format(cal.getTime)
    date
  }

  def getBeforeMonth: String = {

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -30)
    val date = dateFormat.format(cal.getTime)
    date
  }

  def getBeforeQuarter: String = {

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -90)
    val date = dateFormat.format(cal.getTime)
    date
  }

  def getBeforeYear: String = {

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -365)
    val date = dateFormat.format(cal.getTime)
    date
  }
}
