package com.niuzj.test;

import com.niuzj.util.RestClientUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @author niuzj
 * @date 2019/4/19 14:22
 */
public class SearchRequestTest {

    private RestHighLevelClient restHighLevelClient = RestClientUtil.getHighLevelRestClient();

    /**
     * SearchRequest复杂查询
     */
    @Test
    public void matchAll() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //设置查询超时时间
        searchSourceBuilder.timeout(new TimeValue(1000, TimeUnit.MILLISECONDS));
        //分页 from 开始下标 size 每页大小
        searchSourceBuilder.from(1).size(1);
        SearchRequest searchRequest = new SearchRequest("db1");
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        if (response.status().getStatus() < 300) {
            SearchHit[] hits = response.getHits().getHits();
            Stream.of(hits).forEach((hit) -> {
                //文档id
                String id = hit.getId();
                //文档type
                String type = hit.getType();
                //文档index
                String index = hit.getIndex();
                //得到文档的json格式
                String sourceAsString = hit.getSourceAsString();
                //得到评分
                float score = hit.getScore();
                //得到文档的Map
                Map<String, Object> map = hit.getSourceAsMap();
                System.out.println(id);
                System.out.println(type);
                System.out.println(index);
                System.out.println(sourceAsString);
                System.out.println(score);
                System.out.println(map);
            });
        }
    }

    /**
     * 分组
     */
    @Test
    public void aggregations() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("by_last_name").field("last_name.keyword");
        aggregationBuilder.subAggregation(AggregationBuilders.avg("average_age").field("age"));
        searchSourceBuilder.aggregation(aggregationBuilder);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = response.getAggregations();
        aggregations.getAsMap();
        Terms lastNameTerm = aggregations.get("by_last_name");
        List<? extends Terms.Bucket> buckets = lastNameTerm.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            Aggregations bucketAggregations = bucket.getAggregations();
            bucketAggregations.getAsMap();
            Avg avg = bucketAggregations.get("average_age");
            System.out.println(key + ":" + avg.getValue());
        }
    }

    /**
     * 滚动获取数据
     * 设置一个size
     * 第一次获取数据后得到scrollId,后面使用scrollId继续抓取数据,抓取数据的个数依旧是size
     * 直到获取到最后一批数据,scrollId会变为null
     */
    @Test
    public void test03() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(1);
        SearchRequest searchRequest = new SearchRequest("db1");
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(1L));
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(searchResponse.getHits().getTotalHits());
        String scrollId = searchResponse.getScrollId();
        System.out.println(Arrays.toString(searchResponse.getHits().getHits()));
        for ( ; scrollId != null; ) {
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll(TimeValue.timeValueMinutes(1L));
            SearchResponse searchScrollResponse = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
            scrollId = searchScrollResponse.getScrollId();
            SearchHits hits = searchScrollResponse.getHits();
            SearchHit[] hits1 = hits.getHits();
            if (hits1 == null || hits1.length == 0){
                break;
            }
            System.out.println(Arrays.toString(hits1));

        }
    }


    @After
    public void after() throws IOException {
        restHighLevelClient.close();
    }
}
