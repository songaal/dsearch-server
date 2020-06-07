package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.entity.DocumentPagination;
import com.danawa.fastcatx.server.entity.ReferenceTemp;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
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
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service
public class ReferenceService {
    private static Logger logger = LoggerFactory.getLogger(DictionaryService.class);

    private IndicesService indicesService;

    private RestHighLevelClient client;
    private String referenceIndex;

    private final String REFERENCE_INDEX_JSON = "reference.json";

    public ReferenceService(@Qualifier("getRestHighLevelClient") RestHighLevelClient restHighLevelClient,
                            @Value("${fastcatx.reference.index}") String referenceIndex,
                            IndicesService indicesService) {
        this.indicesService = indicesService;
        this.client = restHighLevelClient;
        this.referenceIndex = referenceIndex;
    }

    @PostConstruct
    public void init() throws IOException {
        /* 프로그램 실행시 인덱스 없으면 자동 생성. */
        if (!client.indices().exists(new GetIndexRequest(referenceIndex), RequestOptions.DEFAULT)) {
            String source = StreamUtils.copyToString(new ClassPathResource(REFERENCE_INDEX_JSON).getInputStream(),
                    Charset.defaultCharset());
            client.indices().create(new CreateIndexRequest(referenceIndex)
                            .source(source, XContentType.JSON),
                    RequestOptions.DEFAULT);
        }
    }

    public List<ReferenceTemp> findAll() throws IOException {
        List<ReferenceTemp> referenceTempList = new ArrayList<>();
        Scroll scroll = new Scroll(new TimeValue(1000, TimeUnit.MILLISECONDS));
        SearchResponse response = client.search(new SearchRequest()
                .indices(referenceIndex)
                .scroll(scroll)
                .source(new SearchSourceBuilder()
                        .query(new MatchAllQueryBuilder())
                ), RequestOptions.DEFAULT);

        SearchHit[] SearchHitArr = response.getHits().getHits();
        int hitsSize = SearchHitArr.length;
        for (int i = 0; i < hitsSize; i++) {
            Map<String, Object> source = SearchHitArr[i].getSourceAsMap();
            referenceTempList.add(convertMapToObject(source));
        }
        return referenceTempList;
    }

    public ReferenceTemp find(String id) throws IOException {
        GetResponse response = client.get(new GetRequest().index(referenceIndex).id(id), RequestOptions.DEFAULT);
        return convertMapToObject(response.getSourceAsMap());
    }

    private ReferenceTemp convertMapToObject(Map<String, Object> source) {
        if (source == null) {
            return null;
        }
        ReferenceTemp temp = new ReferenceTemp();
        temp.setName(String.valueOf(source.get("name")));
        temp.setIndices(String.valueOf(source.get("indices")));
        temp.setQuery(String.valueOf(source.get("query")));
        temp.setTitle(String.valueOf(source.get("title")));
        temp.setThumbnails(String.valueOf(source.get("thumbnails")));
        temp.setOrder(String.valueOf(source.get("order")));
        temp.setFields((List<ReferenceTemp.Field>) source.get("fields"));
        temp.setAggs((List<ReferenceTemp.Field>) source.get("aggs"));
        return temp;
    }

    public void add(ReferenceTemp entity) throws IOException {
        client.index(new IndexRequest()
                        .index(referenceIndex)
                        .source(build(entity))
                , RequestOptions.DEFAULT);
    }

    public void update(String id, ReferenceTemp entity) throws IOException {
        client.index(new IndexRequest()
                        .index(referenceIndex)
                        .id(id)
                        .source(build(entity))
                , RequestOptions.DEFAULT);
    }

    private XContentBuilder build(ReferenceTemp entity) throws IOException {
        return jsonBuilder()
                .startObject()
                .field("name", entity.getName())
                .field("indices", entity.getIndices())
                .field("query", entity.getQuery())
                .field("title", entity.getTitle())
                .field("thumbnails", entity.getThumbnails())
                .field("order", entity.getOrder())
                .field("fields", entity.getFields())
                .field("aggs", entity.getAggs())
                .endObject();
    }

    public void delete(String id) throws IOException {
        client.delete(new DeleteRequest().index(referenceIndex).id(id)
                , RequestOptions.DEFAULT);
    }

    public Map<ReferenceTemp, DocumentPagination> searchResponseAll(String keyword) throws IOException {
        Map<ReferenceTemp, DocumentPagination> result = new HashMap<>();
        List<ReferenceTemp> tempList = findAll();
        int size = tempList.size();
        for (int i = 0; i < size; i++) {
            ReferenceTemp temp = tempList.get(i);
            String query = temp.getQuery().replace("${keyword}", keyword);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
            try (XContentParser parser = XContentFactory
                    .xContent(XContentType.JSON)
                    .createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, query)) {
                searchSourceBuilder.parseXContent(parser);
                DocumentPagination documentPagination = indicesService.findAllDocumentPagination(temp.getIndices(), 0, 100, searchSourceBuilder);
                result.put(temp, documentPagination);
            }
        }
        return result;
    }

    public Map<ReferenceTemp, DocumentPagination> searchResponse(String id, String keyword, long pageNum, long rowSize) throws IOException {
        Map<ReferenceTemp, DocumentPagination> result = new HashMap<>();
        ReferenceTemp temp = find(id);
        String query = temp.getQuery().replace("${keyword}", keyword);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        try (XContentParser parser = XContentFactory
                .xContent(XContentType.JSON)
                .createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, query)) {
            searchSourceBuilder.parseXContent(parser);
            DocumentPagination documentPagination = indicesService.findAllDocumentPagination(temp.getIndices(), pageNum, rowSize, searchSourceBuilder);
            result.put(temp, documentPagination);
        }
        return result;
    }

}
