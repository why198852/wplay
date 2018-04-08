package com.wplay.elastic.oper

import com.wplay.elastic.query.{QueryData, QueryOper}
import com.wplay.elastic.util.Kits
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.elasticsearch.search.sort.{SortBuilders, SortOrder}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
/**
  * Created by wangy3 on 2016/12/29.
  *
  * ES 查询引擎
  */
object ESQueryEngine extends Engine[SearchResponse] {

  private val client = ESClientSingleton.getInstance()
  private val log: Logger = LoggerFactory.getLogger(getClass)

  /**
    * 查询
    *
    * @param queryData
    * @return
    */
  def query(queryData: QueryData): SearchResponse = {

    val srb: SearchRequestBuilder = client.prepareSearch(queryData.getIndex).setSize(queryData.getSize)

    if (!Kits.isEmpty(queryData.getType)){
      var s = queryData.getType
      if (s.startsWith("ul_w")) s = s.replaceAll("ul_w", "ul_info_w")
      else if (s.startsWith("ul_m")) s = s.replaceAll("ul_m", "ul_info_m")
      else if (s.startsWith("ul_live_d")) s = s.replaceAll("ul_live_d", "live_info")
      else if (s.startsWith("ul_d")) s = s.replaceAll("ul_d", "ul_info")
      else if (s.startsWith("ul_all")) s = s.replaceAll("ul_all", "ul_info_all")
      srb.setTypes(s.split(","): _*)
    }

    if(queryData.getSelect.nonEmpty){
      val select: Array[String] = queryData.getSelect.toList.toArray
      srb.setFetchSource(select, null)
    }


    fillWhere(queryData, srb)
    val agg_step=fillStep(queryData,srb)
    val agg_group=fillGroup(queryData, srb)

    fillSort(queryData, srb)
    fillStep(queryData, srb)

    if(null!=agg_step&&null!=agg_group){
      srb.addAggregation(agg_step.subAggregation(agg_group))
    }else if(null!=agg_step && null==agg_group){
      srb.addAggregation(agg_step)
    }else if(null==agg_step && null!=agg_group){
      srb.addAggregation(agg_group)
    }
    //    srb.addAggregation(agg_step)
    println(s"execute es DSL:\n ${srb.toString}")
    srb.execute.actionGet
  }
  def main(args: Array[String]): Unit = {
    val queryData = new QueryData()

    queryData.setIndex("sla_nginx_web_v1")
    queryData.setSize(0)
    queryData.setWhere(List(new QueryOper("domain", "equal", "webapi.busi.wplay.cn")))

    queryData.setGroup(List("apiPath"))
    //queryData.setStat("sum_a,sum_c,sum_d")

    val response = ESQueryEngine.query(queryData)


    println(response)

  }


  private def fillGroup(qd: QueryData, srb: SearchRequestBuilder):TermsAggregationBuilder={

    val groups = qd.getGroup

    if (Kits.isEmpty(groups)) return null

    val headAgg = AggregationBuilders.terms(groups.get(0)).field(groups.get(0))
      .size(qd.getSize)
    // .field(groups.get(0))
    //      .order(Terms.Order.aggregation("_term", true))


    val tailAgg = generateAggre(headAgg, groups.tail.toList)


    if (tailAgg != null) {
      qd.getStat.split(",").foreach(stat => {
        val statInfo = stat match {
          // 数量统计
          case "count" => AggregationBuilders.count(stat)
          // 趋势统计
          case "trend" => AggregationBuilders.dateHistogram("trend").field("load_time").dateHistogramInterval(new DateHistogramInterval("1h"))
          case x if x.startsWith("sum_") => AggregationBuilders.sum(stat).field(stat.substring(4))
          case x if x.startsWith("avg_") => AggregationBuilders.avg(stat).field(stat.substring(4))
          case x if x.startsWith("max_") => AggregationBuilders.max(stat).field(stat.substring(4))
          case x if x.startsWith("min_") => AggregationBuilders.min(stat).field(stat.substring(4))
          case other => null
        }
        if (statInfo != null) tailAgg.subAggregation(statInfo)
      })
      headAgg
      //      srb.addAggregation(headAgg)
    }else{
      null
    }
  }


  def fillStep(qd: QueryData, srb: SearchRequestBuilder): RangeAggregationBuilder={
    if(null==qd.getStep){
      return null
    }else{
      val aggregation = AggregationBuilders.range("agg").field(qd.getStep.getField)
      val str=qd.getStep.getVal
      for( s <- str.split(",")){
        aggregation.addRange(s.split("~")(0).toFloat,s.split("~")(1).toFloat)
      }
      aggregation
    }

  }

  private def fillSort(qd: QueryData, srb: SearchRequestBuilder){
    if(Kits.isEmpty(qd.getSort))return
    var sortOrder:SortOrder =SortOrder.DESC
    if(qd.getOrderby.equals("asc")){
      sortOrder=SortOrder.ASC
    }
    for (sort <- qd.getSort) {
      srb.addSort(SortBuilders.fieldSort(sort).order(sortOrder));
    }


  }
  /**
    * 拼装 过滤条件
    * 红大师
    * @param queryData
    * @param srb
    */
  private def fillWhere(queryData: QueryData, srb: SearchRequestBuilder) {

    if (!Kits.isEmpty(queryData.getWhere)) {
      val baseBuilder: BoolQueryBuilder = new BoolQueryBuilder
      val queryBuilder: BoolQueryBuilder = new BoolQueryBuilder


      queryData.getAndOr match {
        case "and" =>
          baseBuilder.must(queryBuilder)
        case "or" =>
          baseBuilder.should(queryBuilder)
      }

      queryData.getWhere.foreach(oper => {
        oper.getOper match {
          case "equal" if oper.getVal != null =>
            queryBuilder.must(QueryBuilders.termsQuery(oper.getField, oper.getVal.toString.split(","): _*))
          case "fuzzy" if oper.getVal != null =>
            queryBuilder.must(QueryBuilders.wildcardQuery(oper.getField, "*" + oper.getVal.toString +"*"))
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
      })
      srb.setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(queryBuilder)
    }
  }


  /**
    * 生成聚合
    *
    * @return
    */
  private def generateAggre(agg: TermsAggregationBuilder, groups: List[String]): TermsAggregationBuilder = {

    if (groups.isEmpty) agg
    else {
      val _agg = AggregationBuilders.terms(groups.head).field(groups.head)
      agg.subAggregation(_agg)
      generateAggre(_agg, groups.tail)
    }
  }
}
