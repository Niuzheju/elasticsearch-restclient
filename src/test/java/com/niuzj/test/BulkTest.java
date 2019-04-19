package com.niuzj.test;

import com.niuzj.util.RestClientUtil;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author niuzj
 * @date 2019/4/19 14:00
 */
public class BulkTest {

    /**
     * bulk批量操作
     */
    @Test
    public void bulkUpdate() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        Map<String, Object> map = new HashMap<>(10);
        map.put("first_name", "jack");
        map.put("last_name", "smith");
        map.put("age", 22);
        map.put("about", "i like fight");
        map.put("interests", new String[]{"music"});
        bulkRequest.add(new IndexRequest("db1").id("4").source(map));
        map.put("age", 30);
        bulkRequest.add(new UpdateRequest("db1", "2").doc(map));
        RestHighLevelClient client = RestClientUtil.getHighLevelRestClient();
        BulkResponse responses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        for (BulkItemResponse itemResponse : responses) {
            if (itemResponse.isFailed()) {
                BulkItemResponse.Failure failure = itemResponse.getFailure();
                continue;
            }
            DocWriteResponse docWriteResponse = itemResponse.getResponse();
            System.out.println("response:" + docWriteResponse);
            DocWriteRequest.OpType opType = itemResponse.getOpType();
            if (DocWriteRequest.OpType.CREATE == opType || DocWriteRequest.OpType.INDEX == opType) {
                IndexResponse indexResponse = (IndexResponse) docWriteResponse;
            } else if (DocWriteRequest.OpType.DELETE == opType) {
                DeleteResponse deleteResponse = (DeleteResponse) docWriteResponse;
            } else if (DocWriteRequest.OpType.UPDATE == opType) {
                UpdateResponse updateResponse = (UpdateResponse) docWriteResponse;
            }
        }
        client.close();
    }

}
