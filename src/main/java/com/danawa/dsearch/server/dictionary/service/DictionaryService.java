package com.danawa.dsearch.server.dictionary.service;

import com.danawa.dsearch.server.clusters.entity.Cluster;
import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.dictionary.entity.DictionarySetting;
import com.danawa.dsearch.server.excpetions.ServiceException;
import com.danawa.dsearch.server.indices.entity.DocumentPagination;
import com.danawa.dsearch.server.indices.service.IndicesService;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
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
    private String dictionaryIndex;
    private String dictionaryApplyIndex;
    private final String INDEX_JSON = "dictionary.json";
    private final String DICT_APPLY_JSON = "dictionary_apply.json";
    private final ElasticsearchFactory elasticsearchFactory;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public DictionaryService(@Value("${dsearch.dictionary.index}") String dictionaryIndex,
                             @Value("${dsearch.dictionary.apply}") String dictionaryApplyIndex,
                             IndicesService indicesService,
                             ClusterService clusterService,
                             ElasticsearchFactory elasticsearchFactory) {
        this.indicesService = indicesService;
        this.dictionaryIndex = dictionaryIndex;
        this.dictionaryApplyIndex = dictionaryApplyIndex;
        this.clusterService = clusterService;
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public void initialize(UUID clusterId) throws IOException {
        indicesService.createSystemIndex(clusterId, dictionaryIndex, INDEX_JSON);
        indicesService.createSystemIndex(clusterId, dictionaryApplyIndex, DICT_APPLY_JSON);
    }

    public List<DictionarySetting> getAnalysisPluginSettings(UUID clusterId) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            List<DictionarySetting> settings = new ArrayList<>();
            Request request = new Request("GET", "/_analysis-product-name/info-dict");
            Response response = client.getLowLevelClient().performRequest(request);
            if (response.getEntity() != null) {
                String body = EntityUtils.toString(response.getEntity());
                Map<String, Object> bodyMap = new Gson().fromJson(body, new TypeToken<Map<String, Object>>(){}.getType());
                if(bodyMap.get("dictionary") != null) {
                    for (Map<String, Object> dictSetting : (List<Map<String, Object>>) bodyMap.get("dictionary")) {
                        DictionarySetting tmp = new DictionarySetting();
                        tmp.setId(dictSetting.get("type") == null ? "" : dictSetting.get("type").toString());
                        if ("SYSTEM".equalsIgnoreCase(tmp.getId())) {
                            tmp.setType("PRODUCT");
                        } else {
                            tmp.setType(dictSetting.get("dictType") == null ? null : dictSetting.get("dictType").toString());
                        }
                        tmp.setName(dictSetting.get("label") == null ? null : dictSetting.get("label").toString());
                        tmp.setIgnoreCase(dictSetting.get("ignoreCase") == null ? null : dictSetting.get("ignoreCase").toString());
                        tmp.setTokenType(dictSetting.get("tokenType") == null ? null : dictSetting.get("tokenType").toString());
                        tmp.setIndex(dictSetting.get("seq") == null ? null : (int) Double.parseDouble(dictSetting.get("seq").toString()));
                        tmp.setDocumentId(tmp.getId());
                        tmp.setCount(dictSetting.get("count") == null ? null : (int) Double.parseDouble(dictSetting.get("count").toString()));
                        tmp.setIndexCount(dictSetting.get("indexCount") == null ? null : (int) Double.parseDouble(dictSetting.get("indexCount").toString()));
                        tmp.setWords(dictSetting.get("words") == null ? null : (int) Double.parseDouble(dictSetting.get("words").toString()));
                        tmp.setUpdatedTime(dictSetting.get("updatedTime") == null ? null : dictSetting.get("updatedTime").toString());
                        tmp.setAppliedTime(dictSetting.get("appliedTime") == null ? null : dictSetting.get("appliedTime").toString());

                        tmp.setColumns(new ArrayList<>());
                        if (dictSetting.get("dictType") != null) {
                            String dictType = dictSetting.get("dictType").toString();
                            if ("SET".equalsIgnoreCase(dictType)) {
                                tmp.getColumns().add(new DictionarySetting.Column("keyword", "단어"));
                            } else if ("SYNONYM".equalsIgnoreCase(dictType)) {
                                tmp.getColumns().add(new DictionarySetting.Column("keyword", "단어"));
                                if ("SYNONYM".equalsIgnoreCase(tmp.getId())) {
                                    tmp.getColumns().add(new DictionarySetting.Column("value", "유사어"));
                                } else {
                                    tmp.getColumns().add(new DictionarySetting.Column("value", "값"));
                                }
                            } else if ("SPACE".equalsIgnoreCase(dictType)) {
                                tmp.getColumns().add(new DictionarySetting.Column("keyword", "단어"));
                            } else if ("COMPOUND".equalsIgnoreCase(dictType)) {
                                tmp.getColumns().add(new DictionarySetting.Column("keyword", "단어"));
                                tmp.getColumns().add(new DictionarySetting.Column("value", "값"));
                            } else if ("SYNONYM_2WAY".equalsIgnoreCase(dictType)) {
                                if ("UNIT_SYNONYM".equalsIgnoreCase(tmp.getId())) {
                                    tmp.getColumns().add(new DictionarySetting.Column("value", "유사어"));
                                } else {
                                    tmp.getColumns().add(new DictionarySetting.Column("value", "값"));
                                }
                            } else if ("CUSTOM".equalsIgnoreCase(dictType)) {
                                tmp.getColumns().add(new DictionarySetting.Column("id", "아이디"));
                                tmp.getColumns().add(new DictionarySetting.Column("keyword", "단어"));
                                tmp.getColumns().add(new DictionarySetting.Column("value", "값"));
                            }
                        }
                        settings.add(tmp);
                    }
                }
            }
            return settings;
        }
    }

    public DocumentPagination documentPagination(UUID clusterId, String type, long pageNum, long rowSize, boolean isMatch, String searchColumns, String value) throws IOException {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().filter(new TermQueryBuilder("type", type));

        if (value != null && !"".equals(value)) {
            // 검색이 있을 경우.
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

    public IndexResponse createDocument(UUID clusterId, Map<String,Object> document) throws IOException, ServiceException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            String type = (String) Objects.requireNonNull(document.get("type")) ;
            String id = (String) Objects.requireNonNull(document.get("id"));
            String keyword = (String) Objects.requireNonNull(document.get("keyword")) ;
            String value = (String) Objects.requireNonNull(document.get("value")) ;

            BoolQueryBuilder queryBuilder = new BoolQueryBuilder()
                    .filter(new MatchQueryBuilder("type", type));

            if (id.length() > 0) {
                queryBuilder.must(new MatchQueryBuilder("id", id));
            }
            if (keyword.length() > 0) {
                queryBuilder.must(new MatchQueryBuilder("keyword", keyword));
            }

            if (value.length() > 0) {
                queryBuilder.must(new TermQueryBuilder("value", value));
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
                    .field("type", type)
                    .field("id", id)
                    .field("keyword", keyword)
                    .field("value", value)
                    // 아래 두개 필드 추가
                    .field("createdTime", new Date())
                    .field("updatedTime", new Date())
                    .endObject();

            IndexResponse indexResponse = client.index(new IndexRequest()
                            .index(dictionaryIndex)
                            .source(builder)
                    , RequestOptions.DEFAULT);

            refreshUpdateTime(clusterId, type);

            updateDocumentDictApplyIndex(dictionaryApplyIndex, clusterId, type);

            return indexResponse;
        }
    }

    public DeleteResponse deleteDocument(UUID clusterId, String dictionary, String id) throws IOException, ServiceException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            GetResponse response = client.get(new GetRequest().index(dictionaryIndex).id(id), RequestOptions.DEFAULT);
            String type = String.valueOf(response.getSourceAsMap().get("type"));
            if (!type.equalsIgnoreCase(dictionary)) {
                throw new ServiceException("Document NotFound Exception");
            }

            DeleteResponse deleteResponse = client.delete(new DeleteRequest()
                            .index(dictionaryIndex)
                            .id(id)
                    , RequestOptions.DEFAULT);

            refreshUpdateTime(clusterId, dictionary);
            updateDocumentDictApplyIndex(dictionaryApplyIndex, clusterId, type);
            return deleteResponse;
        }
    }

    public UpdateResponse updateDocument(UUID clusterId,
                                         String id,
                                         Map<String, Object> request) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("keyword", (String) request.get("keyword"))
                    .field("value", (String) request.get("value"))
                    .field("id", (String) request.get("id"))
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
            updateDocumentDictApplyIndex(dictionaryApplyIndex, clusterId, (String) request.get("type"));
            return updateResponse;
        }
    }

    public StringBuffer download(UUID clusterId, String dictionary) throws IOException {
        List<SearchHit> documentList = findAll(clusterId, dictionary);
        List<DictionarySetting> settings = getAnalysisPluginSettings(clusterId);
        DictionarySetting setting = null;
        for (DictionarySetting dictionarySetting : settings) {
            if (dictionarySetting.getId().equalsIgnoreCase(dictionary)) {
                setting = dictionarySetting;
            }
        }
        StringBuffer sb = new StringBuffer();
        if (setting == null) {
            return sb;
        }
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

    private List<SearchHit> findAll(UUID clusterId, String type) throws IOException {
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
    private void refreshUpdateTime(UUID clusterId, String type) {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            SearchRequest searchRequest = new SearchRequest()
                    .indices(dictionaryApplyIndex).source(new SearchSourceBuilder()
                            .query(new MatchQueryBuilder("id", type))
                            .from(0)
                            .size(1));

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.getHits().getHits() == null || searchResponse.getHits().getHits().length == 0) {
                return;
            }
            SearchHit searchHit = searchResponse.getHits().getHits()[0];

            UpdateRequest updateRequest = new UpdateRequest()
                    .index(dictionaryApplyIndex)
                    .doc(jsonBuilder()
                            .startObject()
                            .field("updatedTime", new Date())
                            .endObject());

            client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.warn("diction updateTime edit fail: {}", e.getMessage());
        }
    }

    public Response findDict(UUID clusterId, Map<String,Object> request) throws IOException{
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            String word = (String) request.get("word");
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

    public Response compileDict(UUID clusterId, Map<String,Object> request) throws IOException{
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            Cluster remoteCluster = clusterService.find(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId));

            String type = (String) request.get("type");
            String method = "POST";
            String endPoint = "/_analysis-product-name/compile-dict";
            String setJson;
            if (!remoteCluster.getId().equals(clusterId)) {
                setJson = "{ \n" +
                        "\"index\": \"" + dictionaryIndex + "\", \n" +
                        "\"exportFile\": true, \n" +
                        "\"distribute\": true, \n" +
                        "\"type\": \"" + type + "\", \n" +
                        "\"host\": \"" + remoteCluster.getHost() + "\", \n" +
                        "\"port\": \"" + remoteCluster.getPort() + "\", \n" +
                        "\"username\": \"" + remoteCluster.getUsername() + "\", \n" +
                        "\"password\": \"" + remoteCluster.getPassword() + "\" \n" +
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
        response.put("username", cluster.getUsername());
        response.put("password", cluster.getPassword());
        response.put("remoteClusterId", remoteClusterId);
        return response;
    }

    public Map<String, Object> insertDictFileToIndex(UUID clusterId, String dictName, String dictType, MultipartFile file, List<String> fields){

        Map<String, Object> result = new HashMap<>();
        int count = 0;
        BulkRequest bulkRequest = new BulkRequest();
        String line = null;

        InputStream in = null;
        BufferedInputStream bis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;

        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            in = file.getInputStream();
            bis = new BufferedInputStream(in);
            isr = new InputStreamReader(bis);
            br = new BufferedReader(isr);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            TimeZone utc = TimeZone.getTimeZone("UTC");
            sdf.setTimeZone(utc);
            Date now = new Date();
            String nowDate = sdf.format(now);

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
                    bulkRequest.requests().clear();
                }
            }

            if( bulkRequest.requests().size() > 0 ){
                logger.info("dictionaryName: {}, {} flushed !!",dictName, count);
                client.bulk(bulkRequest, RequestOptions.DEFAULT);
                bulkRequest.requests().clear();
            }

            updateDocumentDictApplyIndex(dictionaryApplyIndex, clusterId, dictName);
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
        }finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ignore) {
                }
            }
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException ignore) {
                }
            }
            if (isr != null) {
                try {
                    isr.close();
                } catch (IOException ignore) {
                }
            }
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ignore) {

                }
            }
        }
        result.put("result", true);
        result.put("message", "success");
        return result;
    }

    public boolean resetDict(UUID clusterId, String dictName) {
        logger.info("reset dictionaryName: {}", dictName);
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
            deleteByQueryRequest.setQuery(QueryBuilders.matchQuery("type", dictName)).indices(dictionaryIndex);
            client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);

            updateDocumentDictApplyIndex(dictionaryApplyIndex, clusterId, dictName);
        }catch (IOException e){
            logger.error("{}", e);
            return false;
        }
        return true;
    }

    private void updateDocumentDictApplyIndex(String index, UUID clusterId, String id) {
        // 사전 데이터가 변경되거나 추가/삭제 될 때 apply 인덱스의 doc을 업데이트
        try (RestHighLevelClient client = elasticsearchFactory.getClient(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId))) {
            String _id = id.toLowerCase();
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("id", _id)
                    .field("updatedTime", sdf.format(new Date()))
                    .endObject();
            UpdateRequest updateRequest = new UpdateRequest(index, _id).docAsUpsert(true).upsert(builder).doc(builder);
            UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);
            logger.info("status code: {}", response.status().getStatus());
        }catch (Exception e){
            logger.warn("dict_apply updated fail : {}", e.getMessage());
        }
    }
}
