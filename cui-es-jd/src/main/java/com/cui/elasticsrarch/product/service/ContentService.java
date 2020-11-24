package com.cui.elasticsrarch.product.service;

import com.alibaba.fastjson.JSON;
import com.cui.elasticsrarch.product.pojo.Content;
import com.cui.elasticsrarch.product.utils.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * 业务编写
 *
 * @author dfcui
 */
@Service
public class ContentService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    // 解析数据放入es索引中
    public boolean parseContent(String keyWords) throws IOException {
        List<Content> contentList = new HtmlParseUtil().parseJdHtml(keyWords);
        // 把查询的数据批量放入es中
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("2m");
        for (int i = 0; i < contentList.size(); i++) {
            bulkRequest.add(new IndexRequest("taboo_goods")
            .source(JSON.toJSONString(contentList.get(i)), XContentType.JSON));
        }
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        return  bulkResponse.hasFailures();
    }
}
