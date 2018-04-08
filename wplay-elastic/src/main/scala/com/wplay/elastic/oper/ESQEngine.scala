package com.wplay.elastic.oper

import com.wplay.elastic.query.{QueryData, QueryOper, Trend}
import com.wplay.elastic.util.Kits
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.elasticsearch.search.sort.{SortBuilders, SortOrder}

import scala.collection.JavaConversions._
/**
  * Created by wangy3 on 2017/3/30.
  */
object ESQEngine extends Engine[SearchResponse] {

  private lazy val client = ESClientSingleton.getInstance()

  /**
    * 查询
    *
    * @param queryData
    * @return
    */
  def query(queryData: QueryData): SearchResponse = {

    val srb: SearchRequestBuilder = client.prepareSearch(queryData.getIndex).setSize(queryData.getSize)

    // 指定 Type 可以加快查询迅速
    if (!Kits.isEmpty(queryData.getType)) {
      srb.setTypes(queryData.getType.split(","): _*)
    }

    // 指定返回文档的字段 减少网络 IO
    if (queryData.getSelect.nonEmpty) {
      val select: Array[String] = queryData.getSelect.toList.toArray
      srb.setFetchSource(select, null)
    }


    // ------------------------填充过滤条件--------------------------
    if (!Kits.isEmpty(queryData.getWhere)) {
      val baseBuilder: BoolQueryBuilder = new BoolQueryBuilder
      val queryBuilder: BoolQueryBuilder = new BoolQueryBuilder

      for (oper <- queryData.getWhere) {
        oper.getOper match {
          case "equal" if oper.getVal != null =>
            queryBuilder.must(QueryBuilders.termsQuery(oper.getField, oper.getVal.toString.split(","): _*))
          case "fuzzy" if oper.getVal != null =>
            queryBuilder.must(QueryBuilders.wildcardQuery(oper.getField, "*" + oper.getVal.toString + "*"))
          case "notEqual" if oper.getVal != null =>
            queryBuilder.mustNot(QueryBuilders.termsQuery(oper.getField, oper.getVal.toString.split(","): _*))
          case "notEmpty" =>
            queryBuilder.filter(QueryBuilders.existsQuery(oper.getField))
          case "range" =>
            queryBuilder.must(QueryBuilders.rangeQuery(oper.getField)
              .from(oper.getVal match {
                case "" => null
                case other => other
              }).to(oper.getVal2 match {
              case "" => null
              case other => other
            }))
        }
      }

      queryData.getAndOr match {
        case "and" =>
          baseBuilder.must(queryBuilder)
        case "or" =>
          baseBuilder.should(queryBuilder)
      }


      srb.setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(queryBuilder)
    }

    // ------------------------过滤排序--------------------------

    if (!Kits.isEmpty(queryData.getSort)) {
      var sortOrder: SortOrder = SortOrder.DESC
      if (queryData.getOrderby.equals("asc")) {
        sortOrder = SortOrder.ASC
      }

      queryData.getSort.foreach(sort => {
        srb.addSort(SortBuilders.fieldSort(sort).order(sortOrder))
      })

      //      for (sort <- queryData.getSort) {
      //        srb.addSort(SortBuilders.fieldSort(sort).order(sortOrder))
      //      }
    }

    // ------------------------过滤条件填充结束--------------------------

    // 获取 趋势聚合
    val trendAgg = queryData.getTrend match {
      case trend: Trend => Some(AggregationBuilders
        .dateHistogram(trend.getName).field(trend.getField).dateHistogramInterval(new DateHistogramInterval(trend.getInterval)))
      //interval(new DateHistogramInterval(trend.getInterval)))
      case _ => None
    }

    // 填充聚合条件
    val groups = queryData.getGroup

    if (!Kits.isEmpty(groups)) {

      /**
        * 尾递归生成聚合
        *
        * @return
        */
      def generateAgg(headAgg: TermsAggregationBuilder, groups: List[String]): TermsAggregationBuilder = {

        if (groups.nonEmpty) {
          val tailAgg = AggregationBuilders.terms(groups.head).field(groups.head)
          headAgg.subAggregation(tailAgg)
          generateAgg(tailAgg, groups.tail)
        } else {
          headAgg
        }
      }

      val headAgg = AggregationBuilders.terms(groups.get(0)).field(groups.get(0))
        .size(queryData.getSize)
        .field(groups.get(0))
      //.order(Terms.Order.aggregation("_count", false))


      // 递归生成聚合条件，并返回最后一个聚合指针
      val lastAgg = generateAgg(headAgg, groups.tail.toList)

      val _tempAgg = trendAgg match {
        case Some(trend) => lastAgg.subAggregation(trend)
          trend
        case _ => lastAgg
      }

      // 拼装统计算子
      queryData.getStat.split(",").foreach(stat => {
        val statInfo = stat match {
          // 数量统计 druid
          case "count" => AggregationBuilders.count(stat)
          case x if x.startsWith("sum_") => AggregationBuilders.sum(stat).field(stat.substring(4))
          case x if x.startsWith("avg_") => AggregationBuilders.avg(stat).field(stat.substring(4))
          case x if x.startsWith("max_") => AggregationBuilders.max(stat).field(stat.substring(4))
          case x if x.startsWith("min_") => AggregationBuilders.min(stat).field(stat.substring(4))
          case other => null
        }
        if (statInfo != null) _tempAgg.subAggregation(statInfo)
      })
      srb.addAggregation(headAgg)
    } else if (trendAgg.isDefined) {

      val _tempAgg = trendAgg.get
      // 拼装统计算子
      queryData.getStat.split(",").foreach(stat => {
        println(stat)
        val statInfo = stat match {
          // 数量统计 druid
          case "count" => AggregationBuilders.count(stat)
          case x if x.startsWith("sum_") => AggregationBuilders.sum(stat).field(stat.substring(4))
          case x if x.startsWith("avg_") => AggregationBuilders.avg(stat).field(stat.substring(4))
          case x if x.startsWith("max_") => AggregationBuilders.max(stat).field(stat.substring(4))
          case x if x.startsWith("min_") => AggregationBuilders.min(stat).field(stat.substring(4))
          case other => null
        }
        if (statInfo != null) _tempAgg.subAggregation(statInfo)
      })

      srb.addAggregation(_tempAgg)
    }

    println(s"execute es DSL:\n ${srb.toString}")

    queryData.getTimeout match {
      case timeout if timeout > 0 => srb.execute.actionGet(timeout)
      case _ => srb.execute.actionGet
    }
  }


  def main(args: Array[String]): Unit = {

    val queryData = new QueryData()

    queryData.setIndex("origside_pull_online")
    queryData.setSize(10)
    queryData.setWhere(List(
      new QueryOper("times", "range", "2018-01-08 21:00:00", "2018-01-09 11:00:00")
    ))
    //    queryData.setAndOr("or")
    queryData.setStat("count")

    //    queryData.setTrend(new Trend("time_local", "trend", "10s"))
    //    queryData.setGroup(List("time_local", "topic"))
    //    queryData.setStat("sum_numRecords,sum_kafkaNumRecords")


    val response = ESQEngine.query(queryData)

    println(response)

  }
}
