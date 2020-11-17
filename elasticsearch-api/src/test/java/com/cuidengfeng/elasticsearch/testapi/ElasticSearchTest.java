package com.cuidengfeng.elasticsearch.testapi;

import com.alibaba.fastjson.JSON;
import com.cuidengfeng.elasticsearch.entity.User;
import com.google.gson.Gson;
import org.assertj.core.util.Lists;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 崔登峰测试 es7.6.x 高级客户端测试 api
 */
@SpringBootTest
public class ElasticSearchTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    // 测试创建索引
    @Test
    void testCreateIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("cuidengfeng_index");
        CreateIndexResponse createIndexResponse =
                restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    // 测试索引是否存在
    @Test
    void testIndexExists() throws IOException {
        GetIndexRequest indexRequest = new GetIndexRequest(".kibana_task_manager_1");
        boolean exists = restHighLevelClient.indices().exists(indexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    // 测试删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("xui_index");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    // 测试添加文档
    @Test
    void testAddDocument() throws IOException {
        // 创建对象
        User user = new User("崔登峰", 27);
        // 创建请求
        IndexRequest request = new IndexRequest("cui_index");

        // 规则 put/cui_index/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        // 将我们的数据放入请求 json
        request.source(new Gson().toJson(user), XContentType.JSON);

        // 客户端发送请求，获取响应的结果
        IndexResponse index = restHighLevelClient.index
                (request, RequestOptions.DEFAULT);
        System.out.println(index.toString());
        // 对应我们命令返回的状态CREATED
        System.out.println(index.status());

    }

    // 获取文档，判断是否存在 get/index/doc/1
    @Test
    void testIsExists() throws IOException {
        GetRequest getRequest = new GetRequest("cui_index", "1");
        // 不获取返回的_source的上下文了
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    // 获得文档的信息
    @Test
    void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("cui_index", "1");
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        // 打印文档的内容
        System.out.println(getResponse.getSourceAsString());
        // 返回的全部内容和命令式一样的
        System.out.println(getResponse);
    }

    // 更新文档的信息
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("cui_index", "1");
        updateRequest.timeout("1s");

        User user = new User("王素雅", 24);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);

        System.out.println(update.status());
    }

    // 删除文档记录
    @Test
    void testDeleteRequest() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("cuidengfeng", "1");
        deleteRequest.timeout("1s");

        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }

    // 特殊的，真的项目一般都会批量插入数据
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("1s");

        ArrayList<User> users = Lists.newArrayList();
        users.add(new User("崔登峰", 26));
        users.add(new User("王素雅", 24));
        users.add(new User("张晓辉", 25));
        users.add(new User("大王吴", 32));
        users.add(new User("卓子月", 27));

        // 批处理请求
        for (int i = 0; i < users.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("cui_index").id("" + (i + 1))
                            .source(JSON.toJSONString(users.get(i)), XContentType.JSON));
        }
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.status());
    }
}
