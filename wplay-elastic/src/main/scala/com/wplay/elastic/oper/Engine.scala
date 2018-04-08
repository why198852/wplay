package com.wplay.elastic.oper

import com.wplay.elastic.query.QueryData

/**
  *  Created by wangy3 on 2017/1/17.
  */
trait Engine[T] {
  def query(queryData: QueryData): T
}
