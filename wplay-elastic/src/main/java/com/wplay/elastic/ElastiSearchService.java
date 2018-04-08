package com.wplay.elastic;

import com.wplay.elastic.query.QueryOper;
import com.wplay.core.util.StringUtil;
import com.wplay.elastic.oper.ESClientSingleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by wangy3 on 2016/11/22.
 */
public class ElastiSearchService {

    private static final Log log = LogFactory.getLog("elastiSearchService");

    public boolean isIndexExist(String id) {
        try {
            if (ESClientSingleton.getInstance().admin().indices().prepareExists(id).execute().actionGet().isExists()) {
                return true;
            }
        } catch (Exception exception) {
            log.error("isIndexExist: index error", exception);
        }

        return false;
    }

    public IndexResponse createIndex(String index, String type, String id, XContentBuilder jsonData) {
        IndexResponse response = null;
        try {
            response = ESClientSingleton.getInstance().prepareIndex(index, type, id)
                    .setSource(jsonData)
                    .get();
            Thread.sleep(2000);
            return response;
        } catch (Exception e) {
            log.error("createIndex", e);
        }
        return null;
    }

    public UpdateResponse updateIndex(String index, String type, String id, XContentBuilder jsonData) {
        UpdateResponse response = null;
        try {
            System.out.println("updateIndex ");
            response = ESClientSingleton.getInstance().prepareUpdate(index, type, id)
                    .setDoc(jsonData)
                    .execute().get();
            System.out.println("response " + response);
            return response;
        } catch (Exception e) {
            log.error("UpdateIndex", e);
        }
        return null;
    }

    public DeleteResponse removeDocument(String index, String type, String id) {
        DeleteResponse response = null;
        try {
            response = ESClientSingleton.getInstance().prepareDelete(index, type, id).execute().actionGet();
            return response;
        } catch (Exception e) {
            log.error("RemoveIndex", e);
        }
        return null;
    }


    public List<String> getAlldata(MultiGetResponse multiGetResponse) {
        List<String> data = new ArrayList<>();
        try {
            for (MultiGetItemResponse itemResponse : multiGetResponse) {
                GetResponse response = itemResponse.getResponse();
                if (response.isExists()) {
                    String json = response.getSourceAsString();
                    data.add(json);
                }
            }
            return data;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public GetResponse findDocumentByIndex(String index, String type, String id) {
        try {
            GetResponse getResponse = ESClientSingleton.getInstance().prepareGet(index, type, id).get();
            return getResponse;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    public SearchResponse queryDocument(String index, String type, String field, String value) {
        try {
            QueryBuilder queryBuilder = new MatchQueryBuilder(field, value);
            SearchResponse response = ESClientSingleton.getInstance().prepareSearch(index)
                    .setTypes(type)
                    .setSearchType(SearchType.QUERY_AND_FETCH)
                    .setQuery(queryBuilder)
                    .setFrom(0).setSize(3)
//                    .setExplain(true)
                    .execute()
                    .actionGet();
            return response;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }


    public SearchResponse queryDocument(String index, String type, SearchType searchType, String query) {
        Client client = ESClientSingleton.getInstance();
        if (client == null) return null;
        QueryBuilder queryBuilder = new QueryStringQueryBuilder(query);

        SearchRequestBuilder srb = client.prepareSearch(index)
                .setSearchType(searchType)
                .setQuery(queryBuilder);

        if (StringUtil.noEmpty(type)) {
            srb.setTypes(type);
        }

        SearchResponse response = srb.execute().actionGet();
//        client.close();
        return response;
    }

    /**
     * @param index
     * @param type
     * @param queryOpers
     * @return
     */
    public SearchResponse queryDocument(String index, String type, List<QueryOper> queryOpers) {
        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();

            for (QueryOper queryOper : queryOpers) {
                switch (queryOper.getOper()) {
                    case "equal":
                        queryBuilder.must(QueryBuilders.termQuery(queryOper.getField(), queryOper.getVal()));
                        break;
                    case "notEqual":
                        queryBuilder.mustNot(QueryBuilders.termQuery(queryOper.getField(), queryOper.getVal()));
                        break;
                }
            }

            Client client = ESClientSingleton.getInstance();
            if (client == null) return null;

            SearchRequestBuilder srb = client.prepareSearch(index)

                    .setFrom(0).setSize(3).setExplain(true)
                    .setSearchType(SearchType.QUERY_AND_FETCH)
                    .setQuery(queryBuilder);

            if (type != null && !"*".equals(type)) srb.setTypes(type);
            SearchResponse response = srb.execute().actionGet();
//            client.close();
            return response;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }


    /**
     * 漏斗数据查询
     *
     * @param index
     * @param type
     * @param stepQueryOper
     * @return
     */
    public HashMap<Integer, Long> getFunnelData(String index, String type, TreeMap<Integer, QueryOper> stepQueryOper) {

        List<QueryOper> arrayList = new ArrayList<QueryOper>();
        HashMap<Integer, Long> funnelDataMap = new HashMap<Integer, Long>();
        for (Integer key : stepQueryOper.keySet()) {
            arrayList.add(stepQueryOper.get(key));
            queryDocument(index, type, arrayList);
            funnelDataMap.put(key, queryDocument(index, type, arrayList).getHits().totalHits());
        }
        return funnelDataMap;
    }


    /**
     * 查询指定区间 各个Topic 日志接收情况
     *
     * @param from
     * @param to
     * @return
     */
    public SearchResponse aggLogcollect(String from, String to) {

        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
            queryBuilder.must(QueryBuilders.termQuery("startedOrCompleted", "completed"));

            SearchRequestBuilder srb = ESClientSingleton.getInstance().prepareSearch("logcollect");

            srb.setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                    .setQuery(queryBuilder)
                    .addAggregation(
                            AggregationBuilders
                                    //.filter("range_batchTime")
                                    .filter("range_batchTime", QueryBuilders.rangeQuery("batchTime").from(from).to(to))
                                    .subAggregation(
                                            AggregationBuilders.terms("group_by_topic").field("topic")
                                                    .minDocCount(1).size(1000)
                                                    .order(Terms.Order.aggregation("numRecords", false))
                                                    .subAggregation(AggregationBuilders.sum("numRecords").field("numRecords"))
                                    )
                    );

            return srb.execute().actionGet();
        } catch (Exception e) {
            log.error("", e);
        }
        return null;

    }


    public SearchResponse aggLogcollect(String interval, String topic, String from, String to) {

        try {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
            queryBuilder.must(QueryBuilders.termQuery("startedOrCompleted", "completed"));
            if (StringUtil.noEmpty(topic)) {
                queryBuilder.must(QueryBuilders.termQuery("topic", topic));
            }

            SearchRequestBuilder srb = ESClientSingleton.getInstance().prepareSearch("logcollect");

            srb.setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                    .setQuery(queryBuilder)
                    .addAggregation(
                            AggregationBuilders
                                    //.filter("range_batchTime")
                                    .filter("range_batchTime", QueryBuilders.rangeQuery("batchTime").from(from).to(to))
                                    .subAggregation(
                                            AggregationBuilders.dateHistogram("date_histogram")
                                                    .dateHistogramInterval(new DateHistogramInterval(interval))
                                                    //.interval(new DateHistogramInterval(interval))
                                                    .field("batchTime")
                                                    .minDocCount(1)
                                                    .format("MM-dd HH:mm")
                                                    .subAggregation(AggregationBuilders.sum("numRecords").field("numRecords"))
                                                    .subAggregation(AggregationBuilders.sum("flumeNumRecords").field("flumeNumRecords"))
                                                    .subAggregation(AggregationBuilders.sum("kafkaNumRecords").field("kafkaNumRecords"))
                                                    .subAggregation(AggregationBuilders.sum("schedulingDelay").field("schedulingDelay"))
                                                    .subAggregation(AggregationBuilders.sum("processingDelay").field("processingDelay"))
                                    )
                    );
            return srb.execute().actionGet();
        } catch (Exception e) {
            log.error("", e);
        }
        return null;

    }


    public SearchResponse aggGiftInfo(String interval, String index, String from, String to) {

        try {

            SearchRequestBuilder srb = ESClientSingleton.getInstance().prepareSearch(index);

            srb.setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0)
                    .addAggregation(
                            AggregationBuilders
                                    //.filter("time")
                                    .filter("time", QueryBuilders.rangeQuery("time").from(from).to(to))
                                    .subAggregation(
                                            AggregationBuilders.dateHistogram("date_histogram")
                                                    .dateHistogramInterval(new DateHistogramInterval(interval))
                                                    //.interval(new DateHistogramInterval(interval))
                                                    .field("time")
                                                    .minDocCount(1)
                                                    .format("MM-dd HH:mm:ss")
                                                    .subAggregation(
                                                            AggregationBuilders.terms("name").field("name")
                                                                    .subAggregation(AggregationBuilders.sum("num").field("num"))
                                                                    .size(100) //97种礼物类型
                                                    )
                                    )
                    );
            log.info("request expression is "+srb.toString());
            return srb.execute().actionGet();
        } catch (Exception e) {
            log.error("", e);
        }
        return null;

    }

    public static void main(String[] args) {
        ElastiSearchService ess = new ElastiSearchService();
        SearchResponse sr = ess.aggGiftInfo("1m","activity","2018-03-15 22:05:00","2018-03-22 22:20:00");
        System.out.println("response result is "+sr.toString());
    }
}