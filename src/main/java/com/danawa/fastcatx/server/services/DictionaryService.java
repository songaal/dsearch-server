package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.entity.DictDocumentRequest;
import com.danawa.fastcatx.server.entity.DocumentPagination;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
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
import org.elasticsearch.search.sort.SortBuilders;
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
    private final String suffix = ".raw";

    //* 사전 구분 : type
    //사용자 사전: keyword
    //동의어 사전: keyword, synonym
    //불용어 사전: keyword
    //분리어 사전: keyword
    //복합명사 사전: keyword, synonym
    //단위명 사전: keyword
    //단위명 동의어 사전: synonym
    //제조사 사전: id, keyword, synonym
    //브랜드 사전: id, keyword, synonym
    //카테고리 사전: id, keyword, synonym
    //영단어 사전: keyword


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

    public DocumentPagination documentPagination(String type, long pageNum, long rowSize, boolean isMatch, String fields, String value) throws IOException {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder()
                .filter(new TermQueryBuilder("type", type));

        if (fields != null && !"".equals(fields) && value != null && !"".equals(value)) {
            String[] fieldArr = fields.split(",");
            if (fieldArr.length == 1) {
                if (isMatch) {
//                term
                    boolQueryBuilder.must(new TermQueryBuilder(fieldArr[0] + suffix, value));
                } else {
//                wildcard
                    boolQueryBuilder.must(new WildcardQueryBuilder(fieldArr[0] + suffix, "*" + value + "*"));
                }
            } else {
                for (int i = 0; i < fieldArr.length; i++) {
                    if (isMatch) {
                        boolQueryBuilder.must().add(new TermQueryBuilder(fieldArr[i] + suffix, value));
                    } else {
                        boolQueryBuilder.should().add(new WildcardQueryBuilder(fieldArr[i] + suffix, "*" + value + "*"));
                    }

                }
            }
        }

        SearchSourceBuilder builder = new SearchSourceBuilder().query(boolQueryBuilder);

        return indicesService.findAllDocumentPagination(dictionaryIndex, pageNum, rowSize, null, false, builder);
    }

    public IndexResponse createDocument(DictDocumentRequest document) throws IOException {

        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                .filter(new MatchQueryBuilder("type", document.getType().toUpperCase()));

        if (document.getId() != null && !"".equals(document.getId())) {
            queryBuilder.must(new MatchQueryBuilder("id" + suffix, document.getId()));
        }
        if (document.getKeyword() != null && !"".equals(document.getKeyword())) {
            queryBuilder.must(new MatchQueryBuilder("keyword" + suffix, document.getKeyword()));
        }
        if (document.getSynonym() != null && !"".equals(document.getSynonym())) {
            queryBuilder.must(new MatchQueryBuilder("synonym" + suffix, document.getSynonym()));
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder);
        SearchResponse response = client.search(new SearchRequest()
                .indices(dictionaryIndex)
                .source(searchSourceBuilder), RequestOptions.DEFAULT);

        if (response.getHits().getTotalHits().value > 0) {
            return null;
        }

        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("keyword", document.getKeyword())
                .field("synonym", document.getSynonym())
                .field("type", document.getType())
                .field("id", document.getId())
                .endObject();

        return client.index(new IndexRequest().index(dictionaryIndex).source(builder), RequestOptions.DEFAULT);
    }

    public DeleteResponse deleteDocument(String id) throws IOException {
        return client.delete(new DeleteRequest().index(dictionaryIndex).id(id), RequestOptions.DEFAULT);
    }

    public UpdateResponse updateDocument(String id, DictDocumentRequest document) throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("keyword", document.getKeyword())
                .field("synonym", document.getSynonym())
                .field("type", document.getType())
                .field("id", document.getId())
                .endObject();

        UpdateRequest updateRequest = new UpdateRequest()
                .index(dictionaryIndex)
                .id(id)
                .doc(builder);
        return client.update(updateRequest, RequestOptions.DEFAULT);

    }
}
