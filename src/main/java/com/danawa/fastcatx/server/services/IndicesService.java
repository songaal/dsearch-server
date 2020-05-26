package com.danawa.fastcatx.server.services;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.elasticsearch.index.query.QueryBuilders.*;

@Service
public class IndicesService {
    private static Logger logger = LoggerFactory.getLogger(IndicesService.class);
    private RestHighLevelClient client;

    public IndicesService(@Qualifier("getRestHighLevelClient") RestHighLevelClient restHighLevelClient) {
        this.client = restHighLevelClient;
    }


    public Map<String, Object> findAllDocument(String indices, int limit, int size) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indices);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        logger.debug("{}", searchResponse.getHits());

        Map<String, Object> result = new HashMap<>();
        return result;
    }


}
