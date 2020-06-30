package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import com.danawa.fastcatx.server.entity.DictionaryDocumentRequest;
import com.danawa.fastcatx.server.entity.DictionarySetting;
import com.danawa.fastcatx.server.entity.DocumentPagination;
import com.danawa.fastcatx.server.excpetions.ServiceException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
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
public class DictionaryService {
    private static Logger logger = LoggerFactory.getLogger(DictionaryService.class);

    private IndicesService indicesService;

    private String settingIndex;
    private String dictionaryIndex;

    private final String SETTING_JSON = "dictionary_setting.json";
    private final String INDEX_JSON = "dictionary.json";
    private final ElasticsearchFactory elasticsearchFactory;

    public DictionaryService(@Value("${fastcatx.dictionary.setting}") String settingIndex,
                             @Value("${fastcatx.dictionary.index}") String dictionaryIndex,
                             IndicesService indicesService,
                             ElasticsearchFactory elasticsearchFactory) {
        this.indicesService = indicesService;
        this.settingIndex = settingIndex;
        this.dictionaryIndex = dictionaryIndex;
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public void fetchSystemIndex(UUID clusterId) throws IOException {
        indicesService.createSystemIndex(clusterId, settingIndex, SETTING_JSON);
        indicesService.createSystemIndex(clusterId, dictionaryIndex, INDEX_JSON);
    }

    public DictionarySetting getSetting(UUID clusterId, String dictionary) throws IOException {
        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
        SearchResponse response = client.search(new SearchRequest()
                        .indices(settingIndex)
                        .source(new SearchSourceBuilder()
                                .from(0)
                                .size(1)
                                .query(new MatchQueryBuilder("id", dictionary))),
                RequestOptions.DEFAULT);
        SearchHit searchHit = response.getHits().getHits()[0];

        elasticsearchFactory.close(client);
        return fillSettingValue(searchHit);
    }

    public List<DictionarySetting> getSettings(UUID clusterId) throws IOException {
        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
        logger.debug("dictionary setting index: {}", settingIndex);
        List<DictionarySetting> settings = new ArrayList<>();
        SearchResponse response = client.search(new SearchRequest()
                .indices(settingIndex)
                .source(new SearchSourceBuilder().size(10000)), RequestOptions.DEFAULT);
        SearchHit[] searchHits = response.getHits().getHits();
        int hitsSize = searchHits.length;
        for (int i = 0; i < hitsSize; i++) {
            settings.add(fillSettingValue(searchHits[i]));
        }
        elasticsearchFactory.close(client);
        return settings;
    }

    private DictionarySetting fillSettingValue(SearchHit searchHit) {
        Map<String, Object> source = searchHit.getSourceAsMap();
        DictionarySetting setting = new DictionarySetting();
        setting.setId((String) source.get("id"));
        setting.setName((String) source.get("name"));
        setting.setType((String) source.get("type"));
        setting.setIgnoreCase((String) source.get("ignoreCase"));
        setting.setTokenType((String) source.get("tokenType"));
        List<Map> columnList = (List<Map>) source.get("columns");
        List<DictionarySetting.Column> columns = new ArrayList<>();
        for (int i = 0; i < columnList.size(); i++) {
            Map<String, Object> columnMap = (Map<String, Object>) columnList.get(i);
            DictionarySetting.Column column = new DictionarySetting.Column();
            if (columnMap.get("label") != null) {
                column.setLabel(String.valueOf(columnMap.get("label")));
            }
            if (columnMap.get("type") != null) {
                column.setType(String.valueOf(columnMap.get("type")));
            }
            columns.add(column);
        }
        setting.setColumns(columns);
        return setting;
    }

    public List<SearchHit> findAll(UUID clusterId, String type) throws IOException {
        List<SearchHit> documentList = new ArrayList<>();
        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);

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
        elasticsearchFactory.close(client);
        return documentList;
    }

    public DocumentPagination documentPagination(UUID clusterId, String type, long pageNum, long rowSize, boolean isMatch, String searchColumns, String value) throws IOException {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().filter(new TermQueryBuilder("type", type));

        if (value != null && !"".equals(value)) {
//            검색이 있을 경우.
            String[] columns = searchColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                if (isMatch) {
                    boolQueryBuilder.should().add(new MatchQueryBuilder(columns[i], value));
                } else {
                    boolQueryBuilder.should().add(new WildcardQueryBuilder(columns[i], "*" + value + "*"));
                }
            }
            boolQueryBuilder.minimumShouldMatch(1);
        }

        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(boolQueryBuilder)
                .sort(SortBuilders.fieldSort("_id"));

        return indicesService.findAllDocumentPagination(clusterId, dictionaryIndex, pageNum, rowSize, builder);
    }

    public IndexResponse createDocument(UUID clusterId, DictionaryDocumentRequest document) throws IOException, ServiceException {
        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                .filter(new MatchQueryBuilder("type", document.getType()));

        if (document.getId() != null) {
            queryBuilder.must(new MatchQueryBuilder("id", document.getId()));
        }
        if (document.getKeyword() != null) {
            queryBuilder.must(new MatchQueryBuilder("keyword", document.getKeyword()));
        }

        SearchResponse response = client.search(new SearchRequest()
                        .indices(dictionaryIndex)
                        .source(new SearchSourceBuilder().query(queryBuilder))
                , RequestOptions.DEFAULT);

        if (response.getHits().getTotalHits().value > 0) {
            throw new ServiceException("Duplicate Exception");
        }

        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("type", document.getType())
                .field("id", document.getId())
                .field("keyword", document.getKeyword())
                .field("value", document.getValue())
                .endObject();

        IndexResponse indexResponse = client.index(new IndexRequest()
                        .index(dictionaryIndex)
                        .source(builder)
                , RequestOptions.DEFAULT);
        elasticsearchFactory.close(client);
        return indexResponse;
    }

    public DeleteResponse deleteDocument(UUID clusterId, String dictionary, String id) throws IOException, ServiceException {
        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
        GetResponse response = client.get(new GetRequest().index(dictionaryIndex).id(id), RequestOptions.DEFAULT);
        String type = String.valueOf(response.getSourceAsMap().get("type"));
        if (!type .equalsIgnoreCase(dictionary)) {
            throw new ServiceException("Document NotFound Exception");
        }

        DeleteResponse deleteResponse = client.delete(new DeleteRequest()
                        .index(dictionaryIndex)
                        .id(id)
                , RequestOptions.DEFAULT);

        elasticsearchFactory.close(client);
        return deleteResponse;
    }

    public UpdateResponse updateDocument(UUID clusterId, String id, DictionaryDocumentRequest document) throws IOException {
        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("keyword", document.getKeyword())
                .field("value", document.getValue())
                .field("id", document.getId())
                .endObject();

        UpdateResponse updateResponse = client.update(new UpdateRequest()
                        .index(dictionaryIndex)
                        .id(id)
                        .doc(builder)
                , RequestOptions.DEFAULT);
        elasticsearchFactory.close(client);
        return updateResponse;
    }

    public StringBuffer download(UUID clusterId, String dictionary) throws IOException {
        List<SearchHit> documentList = findAll(clusterId, dictionary);
        DictionarySetting setting = getSetting(clusterId, dictionary);

        StringBuffer sb = new StringBuffer();
        int size = documentList.size();
        for (int i = 0; i < size; i++) {
            Map<String, Object> source = documentList.get(i).getSourceAsMap();
            if (setting.getColumns() != null) {
                List<DictionarySetting.Column> columns = setting.getColumns();
                for (int j = 0; j < columns.size(); j++) {
                    sb.append(source.get(columns.get(j).getType()));
                    if (j < columns.size() - 1) {
                        sb.append("\t");
                    }
                }
                sb.append("\r\n");
            }
        }
        return sb;
    }

}
