package com.danawa.dsearch.server.services;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.entity.*;
import com.danawa.dsearch.server.excpetions.ServiceException;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
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
import org.elasticsearch.client.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service
public class DictionaryService {
    private static Logger logger = LoggerFactory.getLogger(DictionaryService.class);

    private IndicesService indicesService;
    private final ClusterService clusterService;

    private String settingIndex;
    private String dictionaryIndex;

    private final String SETTING_JSON = "dictionary_setting.json";
    private final String INDEX_JSON = "dictionary.json";
    private final ElasticsearchFactory elasticsearchFactory;

    public DictionaryService(@Value("${dsearch.dictionary.setting}") String settingIndex,
                             @Value("${dsearch.dictionary.index}") String dictionaryIndex,
                             IndicesService indicesService,
                             ClusterService clusterService,
                             ElasticsearchFactory elasticsearchFactory) {
        this.indicesService = indicesService;
        this.settingIndex = settingIndex;
        this.dictionaryIndex = dictionaryIndex;
        this.clusterService = clusterService;
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public void fetchSystemIndex(UUID clusterId) throws IOException {
        indicesService.createSystemIndex(clusterId, settingIndex, SETTING_JSON);
        indicesService.createSystemIndex(clusterId, dictionaryIndex, INDEX_JSON);
    }

    public DictionarySetting getSetting(UUID clusterId, String dictionary) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            SearchResponse response = client.search(new SearchRequest()
                            .indices(settingIndex)
                            .source(new SearchSourceBuilder()
                                    .from(0)
                                    .size(1)
                                    .query(new MatchQueryBuilder("id", dictionary))),
                    RequestOptions.DEFAULT);
            return fillSettingValue(response.getHits().getHits()[0], response.getHits().getHits()[0].getId());
        }
    }

    public List<DictionarySetting> getSettings(UUID clusterId) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            logger.debug("dictionary setting index: {}", settingIndex);
            List<DictionarySetting> settings = new ArrayList<>();
            SearchResponse response = client.search(new SearchRequest()
                    .indices(settingIndex)
                    .source(new SearchSourceBuilder().size(10000).sort("id")), RequestOptions.DEFAULT);
            SearchHit[] searchHits = response.getHits().getHits();
            int hitsSize = searchHits.length;
            for (int i = 0; i < hitsSize; i++) {
                settings.add(fillSettingValue(searchHits[i], searchHits[i].getId()));
            }
            return settings;
        }
    }

    private DictionarySetting fillSettingValue(SearchHit searchHit, String documentId) {
        Map<String, Object> source = searchHit.getSourceAsMap();
        DictionarySetting setting = new DictionarySetting();
        setting.setDocumentId(documentId);
        setting.setId((String) source.get("id"));
        setting.setName((String) source.get("name"));
        setting.setType((String) source.get("type"));
        setting.setIgnoreCase((String) source.get("ignoreCase"));
        setting.setTokenType((String) source.get("tokenType"));
        setting.setUpdatedTime((String) source.get("updatedTime"));
        setting.setAppliedTime((String) source.get("appliedTime"));
        setting.setIndex((Integer) source.get("index"));

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
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
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
                .trackTotalHits(true)
                .query(boolQueryBuilder)
                .sort(new FieldSortBuilder("updatedTime").order(SortOrder.DESC)) // 추가
                .sort(new FieldSortBuilder("createdTime").order(SortOrder.DESC)) // 추가
                .sort(SortBuilders.fieldSort("_id"));

        return indicesService.findAllDocumentPagination(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId), dictionaryIndex, pageNum, rowSize, builder);
    }

    public IndexResponse createDocument(UUID clusterId, DictionaryDocumentRequest document) throws IOException, ServiceException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                    .filter(new MatchQueryBuilder("type", document.getType()));

            if (document.getId() != null && !"".equals(document.getId())) {
                queryBuilder.must(new MatchQueryBuilder("id", document.getId()));
            }
            if (document.getKeyword() != null && !"".equals(document.getKeyword())) {
                queryBuilder.must(new MatchQueryBuilder("keyword", document.getKeyword()));
            }

            if (document.getValue() != null && !"".equals(document.getValue())) {
                queryBuilder.must(new TermQueryBuilder("value", document.getValue()));
            }

            SearchResponse response = client.search(new SearchRequest()
                            .indices(dictionaryIndex)
                            .source(new SearchSourceBuilder().query(queryBuilder))
                    , RequestOptions.DEFAULT);

            logger.info("{}", response.getHits().getHits());
            if (response.getHits().getTotalHits().value > 0) {
                throw new ServiceException("Duplicate Exception");
            }

            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("type", document.getType())
                    .field("id", document.getId())
                    .field("keyword", document.getKeyword())
                    .field("value", document.getValue())
                    // 아래 두개 필드 추가
                    .field("createdTime", new Date())
                    .field("updatedTime", new Date())
                    .endObject();

            IndexResponse indexResponse = client.index(new IndexRequest()
                            .index(dictionaryIndex)
                            .source(builder)
                    , RequestOptions.DEFAULT);

            refreshUpdateTime(clusterId, document.getType());
            return indexResponse;
        }
    }

    public DeleteResponse deleteDocument(UUID clusterId, String dictionary, String id) throws IOException, ServiceException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            GetResponse response = client.get(new GetRequest().index(dictionaryIndex).id(id), RequestOptions.DEFAULT);
            String type = String.valueOf(response.getSourceAsMap().get("type"));
            if (!type .equalsIgnoreCase(dictionary)) {
                throw new ServiceException("Document NotFound Exception");
            }

            DeleteResponse deleteResponse = client.delete(new DeleteRequest()
                            .index(dictionaryIndex)
                            .id(id)
                    , RequestOptions.DEFAULT);

            refreshUpdateTime(clusterId, dictionary);
            return deleteResponse;
        }
    }

    public UpdateResponse updateDocument(UUID clusterId, String id, DictionaryDocumentRequest document) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("keyword", document.getKeyword())
                    .field("value", document.getValue())
                    .field("id", document.getId())
                    // 아래 한줄 추가
                    .field("updatedTime", new Date())
                    .endObject();

            UpdateResponse updateResponse = client.update(new UpdateRequest()
                            .index(dictionaryIndex)
                            .id(id)
                            .doc(builder)
                    , RequestOptions.DEFAULT);

            GetRequest getRequest = new GetRequest().index(dictionaryIndex).id(id);
            GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
            if (getResponse.getSourceAsMap().get("type") != null && !"".equals(getResponse.getSourceAsMap().get("type"))) {
                String type = String.valueOf(getResponse.getSourceAsMap().get("type"));
                refreshUpdateTime(clusterId, type);
            }
            return updateResponse;
        }
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

//    public SearchResponse getDictionaryTimes(UUID clusterId) throws IOException {
//        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
//            SearchRequest searchRequest = new SearchRequest();
//            searchRequest.indices(settingIndex);
//
//            String query = "{\n" +
//                    "        \"size\": 10000\n" +
//                    "      }";
//            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
//            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), null, query)) {
//                searchSourceBuilder.parseXContent(parser);
//                searchRequest.source(searchSourceBuilder);
//            }
//            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
//            return searchResponse;
//        }
//    }

    public void refreshUpdateTime(UUID clusterId, String type) {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            SearchRequest searchRequest = new SearchRequest()
                    .indices(settingIndex).source(new SearchSourceBuilder()
                            .query(new MatchQueryBuilder("id", type))
                            .from(0)
                            .size(1));

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.getHits().getHits() == null || searchResponse.getHits().getHits().length == 0) {
                return;
            }
            SearchHit searchHit = searchResponse.getHits().getHits()[0];

            UpdateRequest updateRequest = new UpdateRequest()
                    .index(settingIndex)
                    .id(searchHit.getId())
                    .doc(jsonBuilder()
                            .startObject()
                            .field("updatedTime", new Date())
                            .endObject());

            client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.warn("diction updateTime edit fail: {}", e.getMessage());
        }
    }

    public Response findDict(UUID clusterId, DictionarySearchRequest dictionarySearchRequest) throws IOException{
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            String word = dictionarySearchRequest.getWord();
            String method = "POST";
            String endPoint = "/_analysis-product-name/find-dict";
            String setJson = "{ \n" +
                                "\"index\": \"" + dictionaryIndex + "\", \n" +
                                "\"word\": \"" + word + "\"" +
                              "}";
            Request findDictRequest = new Request(method, endPoint);
            findDictRequest.setJsonEntity(setJson);
            return restClient.performRequest(findDictRequest);
        }
    }

    public String getDictionaryInfo(UUID clusterId) throws IOException{
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            Cluster remoteCluster = clusterService.find(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId));

            Request request = new Request("GET", "/_analysis-product-name/info-dict");
            if (!remoteCluster.getId().equals(clusterId)) {
                request.addParameter("host", remoteCluster.getHost());
                request.addParameter("port", String.valueOf(remoteCluster.getPort()));
            }
            Response response = restClient.performRequest(request);
            return EntityUtils.toString(response.getEntity());
        }
    }

    public Response compileDict(UUID clusterId, DictionaryCompileRequest dictionaryCompileRequest) throws IOException{
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            Cluster remoteCluster = clusterService.find(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId));

            String type = dictionaryCompileRequest.getType();
            String method = "POST";
            String endPoint = "/_analysis-product-name/compile-dict";
            String setJson;
            if (!remoteCluster.getId().equals(clusterId)) {
                setJson = "{ \n" +
                        "\"index\": \"" + dictionaryIndex + "\", \n" +
                        "\"exportFile\": true, \n" +
                        "\"distribute\": true, \n" +
                        "\"type\": \"" + type+ "\", \n" +
                        "\"host\": \"" + remoteCluster.getHost() + "\", \n" +
                        "\"port\": \"" + remoteCluster.getPort() + "\" \n" +
                        "}";
            } else {
                setJson = "{ \n" +
                        "\"index\": \"" + dictionaryIndex + "\", \n" +
                        "\"exportFile\": true, \n" +
                        "\"distribute\": true, \n" +
                        "\"type\": \"" + type + "\" \n" +
                        "}";
            }

            logger.info("clusterId: {}, body: {}", clusterId, setJson);
            Request compileDictRequest = new Request(method, endPoint);
            compileDictRequest.setJsonEntity(setJson);
            return restClient.performRequest(compileDictRequest);
        }
    }

    public Map<String, Object> getRemoteInfo(UUID clusterId) {
        Map<String, Object> response = new HashMap<>();
        UUID remoteClusterId = elasticsearchFactory.getDictionaryRemoteClusterId(clusterId);
        Cluster cluster = clusterService.find(remoteClusterId);
        response.put("remote", !remoteClusterId.equals(clusterId));
        response.put("host", cluster.getHost());
        response.put("port", cluster.getPort());
        response.put("remoteClusterId", remoteClusterId);
        return response;
    }

    public void updateTime(UUID clusterId, String id) throws IOException{
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            UpdateRequest updateRequest = new UpdateRequest()
                    .index(settingIndex)
                    .id(id)
                    .doc(jsonBuilder()
                            .startObject()
                            .field("appliedTime", new Date())
                            .endObject());

            client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.warn("dictionary appliedTime edit fail: {}", e.getMessage());
        }
    }

    public void addSetting(UUID clusterId, DictionarySetting setting) throws IOException {
        int index = getSettings(clusterId).size();

        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("id", setting.getId())
                    .field("name", setting.getName())
                    .field("type", setting.getType())
                    .field("tokenType", setting.getTokenType())
                    .field("ignoreCase", setting.getIgnoreCase())
                    .field("index", index)
                    .startArray("columns");

            for (int i = 0; i < setting.getColumns().size(); i++) {
                DictionarySetting.Column column = setting.getColumns().get(i);
                if (column != null && column.getType() != null) {
                    builder.startObject()
                            .field("type", column.getType())
                            .field("label", column.getLabel())
                            .endObject();
                }
            }
            builder.endArray().endObject();

            client.index(new IndexRequest()
                            .index(settingIndex)
                            .source(builder)
                    , RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.warn("dictionary appliedTime edit fail: {}", e.getMessage());
        }
    }

    public void removeSetting(UUID clusterId, String id) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            GetResponse doc = client.get(new GetRequest(settingIndex).id(id), RequestOptions.DEFAULT);
            if(doc.getSourceAsMap() != null && doc.getSourceAsMap().get("id") != null) {
                client.deleteByQueryAsync(new DeleteByQueryRequest(dictionaryIndex)
                                .setQuery(QueryBuilders.matchQuery("type", doc.getSourceAsMap().get("id")))
                        , RequestOptions.DEFAULT
                        , new ActionListener<BulkByScrollResponse>() {
                            @Override public void onResponse(BulkByScrollResponse bulkByScrollResponse) {}
                            @Override public void onFailure(Exception e) {}
                        });
            }
            client.delete(new DeleteRequest(settingIndex).id(id), RequestOptions.DEFAULT);
        }
    }


    public void updatedSettingsList(UUID clusterId, List<DictionarySetting> settings) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {

            for(DictionarySetting setting : settings){
                XContentBuilder builder = jsonBuilder()
                        .startObject()
                        .field("index", setting.getIndex())
                        .endObject();
                UpdateResponse updateResponse = client.update(new UpdateRequest()
                                .index(settingIndex)
                                .id(setting.getDocumentId())
                                .doc(builder)
                        , RequestOptions.DEFAULT);
            }
        }
    }

    public Map<String, Object> insertDictFileToIndex(UUID clusterId, String dictName, String dictType, MultipartFile file, List<String> fields){
        Map<String, Object> result = new HashMap<>();

        int count = 0;
        BulkRequest bulkRequest = new BulkRequest();
        String line = null;
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
//                    String output = new DateTime( DateTimeZone.UTC );
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            TimeZone utc = TimeZone.getTimeZone("UTC");
            sdf.setTimeZone(utc);
            Date now = new Date();
            String nowDate = sdf.format(now);
            InputStream in = file.getInputStream();
            BufferedInputStream bis = new BufferedInputStream(in);
            BufferedReader br = new BufferedReader(new InputStreamReader(bis));
            while((line = br.readLine()) != null){
                String[] split = line.split("\t");
                if(dictType.equals("custom") && split.length == 3){
                    String id = split[0];
                    String keyword = split[1];
                    String value = split[2];

                    Map<String, Object> source = new HashMap<>();
                    source.put(fields.get(0), id);
                    source.put(fields.get(1), keyword);
                    source.put(fields.get(2), value);
                    source.put("createdTime", nowDate);
                    source.put("updatedTime", nowDate);
                    source.put("type", dictName);

                    bulkRequest.add(new IndexRequest().index(dictionaryIndex).source(source));
                    count++;
                }else if(dictType.equals("synonym") && split.length == 2){
                    String keyword = split[0];
                    String value = split[1];

                    Map<String, Object> source = new HashMap<>();
                    source.put(fields.get(0), keyword);
                    source.put(fields.get(1), value);
                    source.put("createdTime", nowDate);
                    source.put("updatedTime", nowDate);
                    source.put("type", dictName);

                    bulkRequest.add(new IndexRequest().index(dictionaryIndex).source(source));
                    count++;
                }else if(dictType.equals("set") && split.length == 1){
                    String keyword = split[0];

                    Map<String, Object> source = new HashMap<>();
                    source.put(fields.get(0), keyword);
                    source.put("createdTime", nowDate);
                    source.put("updatedTime", nowDate);
                    source.put("type", dictName);
                    bulkRequest.add(new IndexRequest().index(dictionaryIndex).source(source));
                    count++;
                }else if(dictType.equals("compound") && split.length == 2){
                    String keyword = split[0];
                    String value = split[1];

                    Map<String, Object> source = new HashMap<>();
                    source.put(fields.get(0), keyword);
                    source.put(fields.get(1), value);
                    source.put("createdTime", nowDate);
                    source.put("updatedTime", nowDate);
                    source.put("type", dictName);
                    bulkRequest.add(new IndexRequest().index(dictionaryIndex).source(source));
                    count++;
                }else if(dictType.equals("space") && split.length == 1){
                    String keyword = split[0];

                    Map<String, Object> source = new HashMap<>();
                    source.put(fields.get(0), keyword);
                    source.put("createdTime", nowDate);
                    source.put("updatedTime", nowDate);
                    source.put("type", dictName);
                    bulkRequest.add(new IndexRequest().index(dictionaryIndex).source(source));
                    count++;
                }else if(dictType.equals("synonym_2way") && split.length == 1){
                    String value = split[0];
                    Map<String, Object> source = new HashMap<>();
                    source.put(fields.get(0), value);
                    source.put("createdTime", nowDate);
                    source.put("updatedTime", nowDate);
                    source.put("type", dictName);
                    bulkRequest.add(new IndexRequest().index(dictionaryIndex).source(source));
                    count++;
                }

                if(count % 1000 == 0){
                    logger.info("dictionaryName: {}, {} flushed !!",dictName, count);
                    client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    bulkRequest = new BulkRequest();
                }
            }
            if( count != 0 ){
                logger.info("dictionaryName: {}, {} flushed !!",dictName, count);
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
            }

            br.close();
            bis.close();
            in.close();
        }catch (IOException e){
            result.put("result", false);
            result.put("message", "IOException");
            logger.error("{}", e);
            return result;
        }catch (ArrayIndexOutOfBoundsException e){
            result.put("result", false);
            result.put("message", "형식에 맞지 않은 파일입니다. [line = " + line + "]");
            logger.error("{}", e);
            return result;
        }
        result.put("result", true);
        result.put("message", "success");
        return result;
    }

    public void resetDict(UUID clusterId, String dictName) {
        logger.info("reset dictionaryName: {}", dictName);
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
            deleteByQueryRequest.setQuery(QueryBuilders.matchQuery("type", dictName)).indices(dictionaryIndex);
            client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
        }catch (IOException e){
            logger.error("{}", e);
        }
    }
}
