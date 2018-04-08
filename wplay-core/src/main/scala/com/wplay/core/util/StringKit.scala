package com.wplay.core.util

/**
  *
  * Created by james on 16/6/27.
  *
  * 字符串工具包
  */
object StringKit{

  /**
    * 空字符串
    *
    * @param string
    * @return
    */
  def isEmpty(string: String): Boolean = {
    string == null || string.isEmpty
  }

  /**
    * 非空字符串
    * @param string
    * @return
    */
  def nonEmpty(string: String): Boolean = {
    !isEmpty(string)
  }
}
