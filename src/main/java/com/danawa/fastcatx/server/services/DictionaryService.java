package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.entity.CreateDictDocumentRequest;
import com.danawa.fastcatx.server.entity.DocumentPagination;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service
public class DictionaryService {
    private static Logger logger = LoggerFactory.getLogger(DictionaryService.class);

    private IndicesService indicesService;

    private RestHighLevelClient client;
    private String dictionaryIndex;

    private final String DICTIONARY_INDEX_JSON = "dictionary.json";

    public DictionaryService(@Qualifier("getRestHighLevelClient") RestHighLevelClient restHighLevelClient,
                             @Value("${fastcatx.dictionary.index}") String dictionaryIndex,
                             IndicesService indicesService) {

        this.indicesService = indicesService;
        this.client = restHighLevelClient;
        this.dictionaryIndex = dictionaryIndex;
    }

    @PostConstruct
    public void init() throws IOException {
        /* 프로그램 실행시 인덱스 없으면 자동 생성. */
        if (!client.indices().exists(new GetIndexRequest(dictionaryIndex), RequestOptions.DEFAULT)) {
            String source = StreamUtils.copyToString(new ClassPathResource(DICTIONARY_INDEX_JSON).getInputStream(),
                    Charset.defaultCharset());
            client.indices().create(new CreateIndexRequest(dictionaryIndex)
                            .source(source, XContentType.JSON),
                    RequestOptions.DEFAULT);
        }
    }

    public List<SearchHit> findAll(String type) throws IOException {
        List<SearchHit> documentList = new ArrayList<>();

        Scroll scroll = new Scroll(new TimeValue(5, TimeUnit.SECONDS));
        SearchResponse response = client.search(new SearchRequest()
                .indices(dictionaryIndex)
                .scroll(scroll)
                .source(new SearchSourceBuilder()
                        .query(new TermQueryBuilder("type", type))
                        .from(0)
                        .size(10000)
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

    public DocumentPagination documentPagination(String type, long pageNum, long rowSize, boolean isMatch, String field, String value) throws IOException {

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder()
                .filter(new TermQueryBuilder("type", type));

        if (field != null && !"".equals(field)
                && value != null && !"".equals(value)) {
            if (isMatch) {
//                term
                boolQueryBuilder.must(new TermQueryBuilder(field, value));
            } else {
//                wildcard
                boolQueryBuilder.must(new WildcardQueryBuilder(field, "*" + value + "*"));
            }
        }
        SearchSourceBuilder builder = new SearchSourceBuilder().query(boolQueryBuilder);

        return indicesService.findAllDocumentPagination(dictionaryIndex, pageNum, rowSize, null, false, builder);
    }

    public void createDocument(CreateDictDocumentRequest document) throws IOException {

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                .filter(new MatchQueryBuilder("type", document.getType().toUpperCase()));

        if (document.getId() != null && !"".equals(document.getId())) {
            queryBuilder.must(new MatchQueryBuilder("id.raw", document.getId()));
        }
        if (document.getKeyword() != null && !"".equals(document.getKeyword())) {
            queryBuilder.must(new MatchQueryBuilder("keyword.raw", document.getKeyword()));
        }
        if (document.getSynonym() != null && !"".equals(document.getSynonym())) {
            queryBuilder.must(new MatchQueryBuilder("synonym.raw", document.getSynonym()));
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder);
        SearchResponse response = client.search(new SearchRequest()
                .indices(dictionaryIndex)
                .source(searchSourceBuilder), RequestOptions.DEFAULT);

        if (response.getHits().getTotalHits().value > 0) {
            return;
        }

        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("keyword", document.getKeyword())
                .field("synonym", document.getSynonym())
                .field("type", document.getType())
                .field("id", document.getId())
                .endObject();

        client.index(new IndexRequest().index(dictionaryIndex).source(builder), RequestOptions.DEFAULT);
    }

    public DeleteResponse deleteDocument(String id) throws IOException {
        return client.delete(new DeleteRequest().index(dictionaryIndex).id(id), RequestOptions.DEFAULT);
    }
}
