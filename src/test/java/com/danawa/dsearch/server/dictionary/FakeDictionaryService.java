package com.danawa.dsearch.server.dictionary;

import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.dictionary.entity.DictionarySetting;
import com.danawa.dsearch.server.dictionary.service.DictionaryService;
import com.danawa.dsearch.server.indices.entity.DocumentPagination;
import com.danawa.dsearch.server.indices.service.IndicesService;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class FakeDictionaryService {

    @Test
    public void test(){
        SearchRequest searchRequest = new SearchRequest("test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.sort("name", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        System.out.println(searchSourceBuilder.toString());
    }
}
