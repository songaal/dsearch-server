package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.entity.DocumentPagination;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class IndicesService {
    private static Logger logger = LoggerFactory.getLogger(IndicesService.class);
    private RestHighLevelClient client;

    public IndicesService(@Qualifier("getRestHighLevelClient") RestHighLevelClient restHighLevelClient) {
        this.client = restHighLevelClient;
    }

    public DocumentPagination findAllDocumentPagination(String indices, int from, int size, String id) throws IOException {
        logger.debug("indices: {}, from: {}, size: {}, id: {}", indices, from, size, id);
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (id != null && !"".equals(id)) {
            searchSourceBuilder
                    .query(QueryBuilders.boolQuery()
                            .must(new TermQueryBuilder("_id", id)));
        } else {
            searchSourceBuilder
                    .from(from)
                    .size(size)
                    .query(QueryBuilders.matchAllQuery());
        }

        searchRequest.indices(indices).source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        DocumentPagination documentPagination = new DocumentPagination();
        documentPagination.setFrom(from);
        documentPagination.setSize(size);
        documentPagination.setHits(searchResponse.getHits());
        documentPagination.setTotalCount(getDocumentCount(indices));
        return documentPagination;
    }

    public long getDocumentCount(String indices) throws IOException {
        return client.count(new CountRequest().indices(indices), RequestOptions.DEFAULT).getCount();
    }


}
