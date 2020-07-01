package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import com.danawa.fastcatx.server.entity.Collection;
import com.danawa.fastcatx.server.entity.ReferenceTemp;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service
public class CollectionService {
    private static Logger logger = LoggerFactory.getLogger(CollectionService.class);

    private final String COLLECTION_INDEX_JSON = "collection.json";
    private final String collectionIndex;
    private final ElasticsearchFactory elasticsearchFactory;
    private final IndicesService indicesService;

    public CollectionService(@Value("${fastcatx.collection.index}") String collectionIndex,
                             ElasticsearchFactory elasticsearchFactory,
                             IndicesService indicesService) {
        this.collectionIndex = collectionIndex;
        this.elasticsearchFactory = elasticsearchFactory;
        this.indicesService = indicesService;
    }

    public void fetchSystemIndex(UUID clusterId) throws IOException {
        indicesService.createSystemIndex(clusterId, collectionIndex, COLLECTION_INDEX_JSON);
    }

    public void add(UUID clusterId, Collection collection) throws IOException {
        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
        client.index(new IndexRequest()
                        .index(collectionIndex)
                        .source(build(collection))
                , RequestOptions.DEFAULT);
        elasticsearchFactory.close(client);
    }

    public List<Collection> findAll(UUID clusterId) throws IOException {
        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
        List<Collection> collectionList = new ArrayList<>();
        SearchResponse response = indicesService.findAll(clusterId, collectionIndex);
        SearchHit[] SearchHitArr = response.getHits().getHits();
        int hitsSize = SearchHitArr.length;
        for (int i = 0; i < hitsSize; i++) {
            Map<String, Object> source = SearchHitArr[i].getSourceAsMap();
            collectionList.add(convertMapToObject(SearchHitArr[i].getId(), source));
        }
        elasticsearchFactory.close(client);
        return collectionList;
    }

    private Collection convertMapToObject(String id, Map<String, Object> source) {
        if (source == null) {
            return null;
        }
        Collection collection = new Collection();
        collection.setId(id);
        collection.setName(String.valueOf(source.get("name")));
        collection.setBaseId(String.valueOf(source.get("baseId")));
        collection.setCron(String.valueOf(source.get("cron")));
        collection.setIndexA(String.valueOf(source.get("indexA")));
        collection.setIndexB(String.valueOf(source.get("indexB")));
        collection.setJdbcId(String.valueOf(source.get("jdbcId")));
        collection.setScheduled(Boolean.parseBoolean(String.valueOf(source.get("scheduled"))));
        Map<String, Object> launcherMap = ((Map<String, Object>) source.get("launcher"));
        Collection.Launcher launcher = new Collection.Launcher();
        if (launcherMap != null) {
            launcher.setParam(String.valueOf(launcherMap.get("param")));
            launcher.setHost(String.valueOf(launcherMap.get("host")));
            launcher.setPort(Integer.parseInt(String.valueOf(launcherMap.get("port"))));
        }
        collection.setLauncher(launcher);
        return collection;
    }

    private XContentBuilder build(Collection collection) throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("name", collection.getName() == null ? "" : collection.getName())
                .field("baseId", collection.getBaseId() == null ? "" : collection.getBaseId())
                .field("indexA", collection.getIndexA() == null ? "" : collection.getIndexA())
                .field("indexB", collection.getIndexB() == null ? "" : collection.getIndexB())
                .field("scheduled", collection.isScheduled())
                .field("jdbcId", collection.getJdbcId() == null ? "" : collection.getJdbcId())
                .field("cron", collection.getCron() == null ? "" : collection.getCron());
        if (collection.getLauncher() != null) {
            Collection.Launcher launcher = collection.getLauncher();
            builder.startObject("launcher")
                    .field("param", launcher.getParam() == null ? "" : launcher.getParam())
                    .field("host", launcher.getHost() == null ? "" : launcher.getHost())
                    .field("port", launcher.getPort() == 0 ? "" : launcher.getPort())
                    .endObject();
        }
        return builder.endObject();
    }


//    public ReferenceTemp find(UUID clusterId, String id) throws IOException {
//        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
//        GetResponse response = client.get(new GetRequest().index(referenceIndex).id(id), RequestOptions.DEFAULT);
//        elasticsearchFactory.close(client);
//        return convertMapToObject(id, response.getSourceAsMap());
//    }

//    public void add(UUID clusterId, ReferenceTemp entity) throws IOException {
//        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
//        client.index(new IndexRequest()
//                        .index(referenceIndex)
//                        .source(build(entity))
//                , RequestOptions.DEFAULT);
//        elasticsearchFactory.close(client);
//    }
//
//    public void update(UUID clusterId, String id, ReferenceTemp entity) throws IOException {
//        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
//        client.index(new IndexRequest()
//                        .index(referenceIndex)
//                        .id(id)
//                        .source(build(entity))
//                , RequestOptions.DEFAULT);
//        elasticsearchFactory.close(client);
//    }
//
//    private XContentBuilder build(ReferenceTemp entity) throws IOException {
//        XContentBuilder builder = jsonBuilder()
//                .startObject()
//                .field("name", entity.getName() == null ? "" : entity.getName())
//                .field("indices", entity.getIndices() == null ? "" : entity.getIndices())
//                .field("query", entity.getQuery() == null ? "" : entity.getQuery())
//                .field("title", entity.getTitle() == null ? "" : entity.getTitle())
//                .field("clickUrl", entity.getClickUrl() == null ? "" : entity.getClickUrl())
//                .field("thumbnails", entity.getThumbnails() == null ? "" : entity.getThumbnails())
//                .field("order", entity.getOrder() == null ? 999 : entity.getOrder());
//
//        builder.startArray("fields");
//        if (entity.getFields() != null) {
//            for (int i = 0; i < entity.getFields().size(); i++) {
//                ReferenceTemp.Field field = entity.getFields().get(i);
//                builder.startObject()
//                        .field("label", field.getLabel())
//                        .field("value", field.getValue())
//                        .endObject();
//            }
//        }
//
//        builder.endArray();
//
//        builder.startArray("aggs");
//        if (entity.getAggs() != null) {
//            for (int i = 0; i < entity.getAggs().size(); i++) {
//                ReferenceTemp.Field aggs = entity.getAggs().get(i);
//                builder.startObject()
//                        .field("label", aggs.getLabel())
//                        .field("value", aggs.getValue())
//                        .endObject();
//            }
//        }
//        return builder.endArray().endObject();
//    }
//
//    public void delete(UUID clusterId, String id) throws IOException {
//        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
//        client.delete(new DeleteRequest().index(referenceIndex).id(id)
//                , RequestOptions.DEFAULT);
//        elasticsearchFactory.close(client);
//    }
//
//    public List<ReferenceResult> searchResponseAll(UUID clusterId, String keyword) throws IOException {
//        List<ReferenceResult> result = new ArrayList<>();
//
//        List<ReferenceTemp> tempList = findAll(clusterId);
//        int size = tempList.size();
//        for (int i = 0; i < size; i++) {
//            ReferenceTemp temp = tempList.get(i);
//            String query = temp.getQuery()
//                    .replace("\"${keyword}\"", "\"" + keyword + "\"")
//                    .replace("${keyword}", "\"" + keyword + "\"");
//            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
//
//            if (!jsonUtils.validate(query)) {
//                continue;
//            }
//
//            try (XContentParser parser = XContentFactory
//                    .xContent(XContentType.JSON)
//                    .createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, query)) {
//                searchSourceBuilder.parseXContent(parser);
//                DocumentPagination documentPagination = indicesService.findAllDocumentPagination(clusterId, temp.getIndices(), 0, 30, searchSourceBuilder);
//                result.add(new ReferenceResult(temp, documentPagination, query));
//            } catch (Exception e) {
//                result.add(new ReferenceResult(temp, new DocumentPagination(), query));
//            }
//        }
//        return result;
//    }
//
//    public ReferenceResult searchResponse(UUID clusterId, String id, String keyword, long pageNum, long rowSize) throws IOException {
//        ReferenceResult result = new ReferenceResult();
//        ReferenceTemp temp = find(clusterId, id);
//        String query = temp.getQuery()
//                .replace("\"${keyword}\"", "\"" + keyword + "\"")
//                .replace("${keyword}", "\"" + keyword + "\"");
//
//        if (!jsonUtils.validate(query)) {
//            return result;
//        }
//
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
//        try (XContentParser parser = XContentFactory
//                .xContent(XContentType.JSON)
//                .createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, query)) {
//            searchSourceBuilder.parseXContent(parser);
//            DocumentPagination documentPagination = indicesService.findAllDocumentPagination(clusterId, temp.getIndices(), pageNum, rowSize, searchSourceBuilder);
//
//            result.setQuery(query);
//            result.setTemplate(temp);
//            result.setDocuments(documentPagination);
//        }
//        return result;
//    }







}
