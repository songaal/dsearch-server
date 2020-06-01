package com.danawa.fastcatx.server.services;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class DictionaryService {
    private static Logger logger = LoggerFactory.getLogger(DictionaryService.class);

    private RestHighLevelClient client;
    private String dictionaryIndex;

    private final int searchSize = 10000;

    public DictionaryService(@Qualifier("getRestHighLevelClient") RestHighLevelClient restHighLevelClient,
                             @Value("${fastcatx.dictionary.index}") String dictionaryIndex) {
        this.client = restHighLevelClient;
        this.dictionaryIndex = dictionaryIndex;
    }

    @PostConstruct
    public void init() throws IOException {
        if (!client.indices().exists(new GetIndexRequest(dictionaryIndex), RequestOptions.DEFAULT)) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(dictionaryIndex);
            createIndexRequest.settings(Settings.builder()
                    .put("number_of_shards", 5)
                    .put("number_of_replicas", 0).build());

            createIndexRequest.mapping("{\n" +
                    "    \"properties\": {\n" +
                    "      \"keyword\": {\n" +
                    "        \"type\": \"text\",\n" +
                    "        \"fields\": {\n" +
                    "          \"raw\": {\n" +
                    "            \"type\": \"keyword\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"synonyms\": {\n" +
                    "        \"type\": \"text\",\n" +
                    "        \"fields\": {\n" +
                    "          \"raw\": {\n" +
                    "            \"type\": \"keyword\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"id\": {\n" +
                    "        \"type\": \"text\",\n" +
                    "        \"fields\": {\n" +
                    "          \"raw\": {\n" +
                    "            \"type\": \"keyword\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"type\": {\n" +
                    "        \"type\": \"keyword\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }", XContentType.JSON);
            client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        }
    }

    public List<SearchHit> findAll(String type) throws IOException {
        List<SearchHit> documentList = new ArrayList<>();

        Scroll scroll = new Scroll(new TimeValue(1000, TimeUnit.MILLISECONDS));
        SearchResponse response = client.search(new SearchRequest()
                .indices(dictionaryIndex)
                .scroll(scroll)
                .source(new SearchSourceBuilder()
                        .query(new TermQueryBuilder("type", type))
                        .size(searchSize)
                ), RequestOptions.DEFAULT);

        while (response.getScrollId() != null && response.getHits().getHits().length > 0) {
            documentList.addAll(Arrays.asList(response.getHits().getHits()));
            response = client.scroll(new SearchScrollRequest()
                    .scroll(scroll)
                    .scrollId(response.getScrollId())
                    , RequestOptions.DEFAULT);
        }

        logger.debug("hits Size: {}", documentList.size());



        return documentList;
    }


}
