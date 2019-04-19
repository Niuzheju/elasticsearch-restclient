package com.niuzj.test;

import com.niuzj.util.RestClientUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author niuzj
 * @date 2019/4/18 11:43
 */
public class BaseCRUDTest {

    private RestHighLevelClient restHighLevelClient = RestClientUtil.getHighLevelRestClient();

    /**
     * 创建RestClient
     */
    @Test
    public void test01() throws IOException {
        RestClient restClient = RestClientUtil.getRestClient();
        System.out.println(restClient);
        restClient.close();
    }

    @Test
    public void search() throws IOException {
        //7.0中每个index只有一个type, 在查询中推荐使用_doc代替type名称
        GetRequest getRequest = new GetRequest("db1").id("1");
        //根据id获取文档
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        //判断文档是否存在,返回boolean类型
        boolean b = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(b);
        System.out.println(getResponse.getSourceAsString());
    }

    /**
     * FetchSourceContext 指定获取的field
     */
    @Test
    public void searchField() throws IOException {
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, new String[]{"age"}, new String[]{});
        GetRequest getRequest = new GetRequest("db1").id("2").fetchSourceContext(fetchSourceContext);
        GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString());
    }

    /**
     * 添加文档
     */
    @Test
    public void add() throws IOException {
        Map<String, Object> map = new HashMap<>(10);
        map.put("first_name", "jack");
        map.put("last_name", "smith");
        map.put("age", 22);
        map.put("about", "i like fight");
        map.put("interests", new String[]{"music"});
        //数据可以是map, 实体类, json字符串, 二进制数据
        IndexRequest indexRequest = new IndexRequest("db1").id("3").source(map);
        restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void update() throws IOException {
        Map<String, Object> map = new HashMap<>(10);
        map.put("age", 22);
        UpdateRequest updateRequest = new UpdateRequest("db1", "2").doc(map);
        restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
    }

    /**
     * 异步更新方法,可以设置回调函数
     * 增删改查都有对应的异步方法, XXXAsync
     */
    @Test
    public void updateAsync() throws InterruptedException {
        Map<String, Object> map = new HashMap<>(10);
        map.put("age", 25);
        UpdateRequest updateRequest = new UpdateRequest("db1", "2").doc(map);
        restHighLevelClient.updateAsync(updateRequest, RequestOptions.DEFAULT, new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
                System.out.println("status=" + updateResponse.status().getStatus());
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        });
        Thread.sleep(100);
    }

    @Test
    public void delete() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("db1", "1");
        DeleteResponse response = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(response.status().getStatus());
    }

    @After
    public void after() throws IOException {
        restHighLevelClient.close();
    }
}
