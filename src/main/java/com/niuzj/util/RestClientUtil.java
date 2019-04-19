package com.niuzj.util;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author niuzj
 * @date 2019/4/19 14:01
 */
public final class RestClientUtil {

    public static RestClientBuilder getRestClientBuilder() {
        String host = "192.168.70.80";
        Integer port = 9200;
        String scheme = "http";
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, scheme));
        //修改httpclient发送请求时的配置
        builder.setRequestConfigCallback(builder1 -> {
            builder1.setSocketTimeout(2000);
            return builder1;
        });
        //创建RestClient
        return builder;
    }

    public static RestHighLevelClient getHighLevelRestClient() {
        //创建RestHighLevelClient
        return new RestHighLevelClient(getRestClientBuilder());
    }

    public static RestClient getRestClient() {
        //创建RestClient
        return getRestClientBuilder().build();
    }

}
