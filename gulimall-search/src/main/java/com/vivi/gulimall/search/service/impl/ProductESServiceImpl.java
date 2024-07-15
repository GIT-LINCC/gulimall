package com.vivi.gulimall.search.service.impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.JsonData;
import com.alibaba.fastjson.JSON;
import com.vivi.common.constant.SearchConstant;
import com.vivi.common.to.SkuESModel;
import com.vivi.gulimall.search.service.ProductESService;
import lombok.extern.slf4j.Slf4j;
//import org.elasticsearch.action.bulk.BulkRequest;
//import org.elasticsearch.action.bulk.BulkResponse;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.RequestOptions;
//import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wangwei
 * 2020/10/22 23:17
 */
@Slf4j
@Service
public class ProductESServiceImpl implements ProductESService {

    @Autowired
    private ElasticsearchClient esClient;

    @Override
    public boolean batchSave(List<SkuESModel> list) throws IOException {
        // request.add(new IndexRequest("posts").id("1")
        //         .source(XContentType.JSON,"field", "foo"));

        // Java API Client 新方法
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (SkuESModel skuESModel : list) {
            br.operations(op -> op
                    .index(idx -> idx
                            .index(SearchConstant.ESIndex.ES_PRODUCT_INDEX)
                            .id(skuESModel.getSkuId().toString())
                            .document(skuESModel)
                    )
            );
        }
        log.info("保存执行完毕");
        br.timeout(Time.of(t -> t.time("2m")));

        BulkResponse bulkResponse = esClient.bulk(br.build());
        if (bulkResponse.errors()) {
            List<String> strings = bulkResponse.items().stream()
                    .filter(item -> item.error() != null)
                    .map(item -> item.id() + ", " + item.error().reason() + "\n")
                    .collect(Collectors.toList());
            log.error("商品sku保存至ES失败: {}", strings);
            return false;
        }
        return true;
    }
}
