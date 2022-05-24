package com.danawa.dsearch.server.reference;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.indices.entity.DocumentPagination;
import com.danawa.dsearch.server.indices.IndicesService;
import com.danawa.dsearch.server.reference.entity.ReferenceOrdersRequest;
import com.danawa.dsearch.server.reference.entity.ReferenceResult;
import com.danawa.dsearch.server.reference.entity.ReferenceTemp;
import com.danawa.dsearch.server.utils.JsonUtils;
import com.google.gson.Gson;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service
public class ReferenceService {
    private static Logger logger = LoggerFactory.getLogger(ReferenceService.class);

    private String referenceIndex;
    private final String REFERENCE_INDEX_JSON = "reference.json";

    private final ElasticsearchFactory elasticsearchFactory;
    private final IndicesService indicesService;
    private final JsonUtils jsonUtils;

    public ReferenceService(@Value("${dsearch.reference.index}") String referenceIndex,
                            IndicesService indicesService,
                            JsonUtils jsonUtils,
                            ElasticsearchFactory elasticsearchFactory) {
        this.elasticsearchFactory = elasticsearchFactory;
        this.indicesService = indicesService;
        this.referenceIndex = referenceIndex;
        this.jsonUtils = jsonUtils;
    }

    public void fetchSystemIndex(UUID clusterId) throws IOException {
        indicesService.createSystemIndex(clusterId, referenceIndex, REFERENCE_INDEX_JSON);
    }

    public List<ReferenceTemp> findAll(UUID clusterId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            List<ReferenceTemp> referenceTempList = new ArrayList<>();
            SearchResponse response = indicesService.findAll(clusterId, referenceIndex);
            SearchHit[] SearchHitArr = response.getHits().getHits();
            int hitsSize = SearchHitArr.length;
            for (int i = 0; i < hitsSize; i++) {
                Map<String, Object> source = SearchHitArr[i].getSourceAsMap();
                referenceTempList.add(convertMapToObject(SearchHitArr[i].getId(), source));
            }
            return referenceTempList;
        }

    }

    public ReferenceTemp find(UUID clusterId, String id) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            GetResponse response = client.get(new GetRequest().index(referenceIndex).id(id), RequestOptions.DEFAULT);
            return convertMapToObject(id, response.getSourceAsMap());
        }
    }

    private ReferenceTemp convertMapToObject(String id, Map<String, Object> source) {
        if (source == null) {
            return null;
        }
        ReferenceTemp temp = new ReferenceTemp();
        temp.setId(id);
        temp.setName(String.valueOf(source.get("name")));
        temp.setIndices(String.valueOf(source.get("indices")));
        temp.setQuery(String.valueOf(source.get("query")));
        temp.setTitle(String.valueOf(source.get("title")));
        temp.setClickUrl(String.valueOf(source.get("clickUrl")));
        temp.setThumbnails(String.valueOf(source.get("thumbnails")));
        temp.setOrder(String.valueOf(source.get("order")));
        temp.setFields((List<ReferenceTemp.Field>) source.get("fields"));
        temp.setAggs((List<ReferenceTemp.Field>) source.get("aggs"));
        return temp;
    }

    public void add(UUID clusterId, ReferenceTemp entity) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            client.index(new IndexRequest()
                            .index(referenceIndex)
                            .source(build(entity))
                    , RequestOptions.DEFAULT);
        }
    }

    public void update(UUID clusterId, String id, ReferenceTemp entity) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            client.index(new IndexRequest()
                            .index(referenceIndex)
                            .id(id)
                            .source(build(entity))
                    , RequestOptions.DEFAULT);
            elasticsearchFactory.close(client);
        }
    }

    private XContentBuilder build(ReferenceTemp entity) throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("name", entity.getName() == null ? "" : entity.getName())
                .field("indices", entity.getIndices() == null ? "" : entity.getIndices())
                .field("query", entity.getQuery() == null ? "" : entity.getQuery())
                .field("title", entity.getTitle() == null ? "" : entity.getTitle())
                .field("clickUrl", entity.getClickUrl() == null ? "" : entity.getClickUrl())
                .field("thumbnails", entity.getThumbnails() == null ? "" : entity.getThumbnails())
                .field("order", entity.getOrder() == null ? 999 : entity.getOrder());

        builder.startArray("fields");
        if (entity.getFields() != null) {
            for (int i = 0; i < entity.getFields().size(); i++) {
                ReferenceTemp.Field field = entity.getFields().get(i);
                builder.startObject()
                        .field("label", field.getLabel())
                        .field("value", field.getValue())
                        .endObject();
            }
        }

        builder.endArray();

        builder.startArray("aggs");
        if (entity.getAggs() != null) {
            for (int i = 0; i < entity.getAggs().size(); i++) {
                ReferenceTemp.Field aggs = entity.getAggs().get(i);
                builder.startObject()
                        .field("label", aggs.getLabel())
                        .field("value", aggs.getValue())
                        .endObject();
            }
        }
        return builder.endArray().endObject();
    }

    public void delete(UUID clusterId, String id) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            client.delete(new DeleteRequest().index(referenceIndex).id(id)
                    , RequestOptions.DEFAULT);
        }
    }

    public List<ReferenceResult> searchResponseAll(UUID clusterId, String keyword) throws IOException {
        List<ReferenceResult> result = new ArrayList<>();

        List<ReferenceTemp> tempList = findAll(clusterId);
        int size = tempList.size();
        for (int i = 0; i < size; i++) {
            ReferenceTemp temp = tempList.get(i);

            String query = temp.getQuery()
                    .replace("\"${keyword}\"", "\"" + keyword + "\"")
                    .replace("${keyword}", keyword);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());

            if (!jsonUtils.validate(query)) {
                logger.info("{}", query);
                continue;
            }

            try (XContentParser parser = XContentFactory
                    .xContent(XContentType.JSON)
                    .createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, query)) {
                searchSourceBuilder.parseXContent(parser);
                logger.info("{}", searchSourceBuilder.toString());
                DocumentPagination documentPagination = indicesService.findAllDocumentPagination(clusterId, temp.getIndices(), 0, 30, searchSourceBuilder);
                result.add(new ReferenceResult(temp, documentPagination, query));
            } catch (Exception e) {
                result.add(new ReferenceResult(temp, new DocumentPagination(), query));
            }
        }
        return result;
    }

    public ReferenceResult searchResponse(UUID clusterId, String id, String keyword, long pageNum, long rowSize) throws IOException {
        ReferenceResult result = new ReferenceResult();
        ReferenceTemp temp = find(clusterId, id);
        String query = temp.getQuery()
                .replace("\"${keyword}\"", "\"" + keyword + "\"")
                .replace("${keyword}", keyword);

        if (!jsonUtils.validate(query)) {
            return result;
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        try (XContentParser parser = XContentFactory
                .xContent(XContentType.JSON)
                .createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, query)) {
            searchSourceBuilder.parseXContent(parser);
            DocumentPagination documentPagination = indicesService.findAllDocumentPagination(clusterId, temp.getIndices(), pageNum, rowSize, searchSourceBuilder);

            result.setQuery(query);
            result.setTemplate(temp);
            result.setDocuments(documentPagination);
        }
        return result;
    }

    public void updateOrders(UUID clusterId, ReferenceOrdersRequest request) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            int size = request.getOrders().size();
            for (int i = 0; i < size; i++) {
                ReferenceOrdersRequest.Order order = request.getOrders().get(i);
                client.update(new UpdateRequest()
                        .index(referenceIndex)
                        .id(order.getId())
                        .doc(jsonBuilder()
                                .startObject()
                                .field("order", order.getOrder())
                                .endObject()), RequestOptions.DEFAULT);

            }
        }
    }

    public String download(UUID clusterId){
        StringBuffer sb = new StringBuffer();
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(referenceIndex).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(10000).from(0));
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();

            Gson gson = new Gson();
            for(SearchHit hit : hits){
                Map<String, Object> body = new HashMap<>();
                body.put("_index", referenceIndex);
                body.put("_type", "_doc");
                body.put("_id", hit.getId());
                body.put("_score", hit.getScore());
                body.put("_source", hit.getSourceAsMap());
                String stringBody = gson.toJson(body);
                logger.info("{}", stringBody);
                sb.append(stringBody);
                sb.append("\n");
            }
        } catch (IOException e) {
            logger.error("{}", e);
        }
        return sb.toString();
    }
}
