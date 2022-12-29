package com.danawa.dsearch.server.jdbc.adapter;

import com.danawa.dsearch.server.jdbc.dto.JdbcUpdateRequest;
import com.danawa.dsearch.server.jdbc.entity.JdbcInfo;
import com.danawa.dsearch.server.jdbc.repository.JdbcRepository;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.search.SearchHit;
import org.h2.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class JdbcElasticsearchAdapter implements JdbcAdapter{
    private static Logger logger = LoggerFactory.getLogger(JdbcElasticsearchAdapter.class);
    private JdbcRepository jdbcRepository;

    public JdbcElasticsearchAdapter(
            JdbcRepository jdbcRepository){
        this.jdbcRepository = jdbcRepository;
    }

    public List<JdbcInfo> findAll(UUID clusterId, String index) throws IOException {
        SearchResponse searchResponse = jdbcRepository.findAll(clusterId, index);
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        return convertSearchResponseToList(searchHits);
    }

    private List<JdbcInfo> convertSearchResponseToList(SearchHit[] response){
        List<JdbcInfo> list = new ArrayList<>();

        for(SearchHit searchHit: response){
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            sourceAsMap.put("_id", searchHit.getId());
            list.add(convertMapToJdbcInfo(sourceAsMap));
        }

        return list;
    }
    private JdbcInfo convertMapToJdbcInfo(Map<String, Object> map){
        JdbcInfo jdbcInfo = new JdbcInfo();
        if(map.get("_id") != null){
            jdbcInfo.set_id((String) map.get("_id"));
        }
        if(map.get("address") != null){
            jdbcInfo.setAddress((String) map.get("address"));
        }

        if(map.get("db_name") != null){
            jdbcInfo.setDb_name((String) map.get("db_name"));
        }

        if(map.get("driver") != null){
            jdbcInfo.setDriver((String) map.get("driver"));
        }

        if(map.get("id") != null){
            jdbcInfo.setId((String) map.get("id"));
        }

        if(map.get("name") != null){
            jdbcInfo.setName((String) map.get("name"));
        }

        if(map.get("password") != null){
            jdbcInfo.setPassword((String) map.get("password"));
        }

        if(map.get("provider") != null){
            jdbcInfo.setProvider((String) map.get("provider"));
        }

        if(map.get("url") != null){
            jdbcInfo.setUrl((String) map.get("url"));
        }

        if(map.get("user") != null){
            jdbcInfo.setUser((String) map.get("user"));
        }

        return jdbcInfo;
    }

    public boolean create(UUID clusterId, String index, JdbcInfo jdbcInfo) throws IOException {
        Map<String, Object> jsonMap = convertJdbcInfoToMap(jdbcInfo);
        IndexResponse indexResponse = jdbcRepository.save(clusterId, index, jsonMap);
        return convertResponseToBoolean(indexResponse);
    }

    private Map<String, Object> convertJdbcInfoToMap(JdbcInfo jdbcInfo){
        Map<String, Object> result = new HashMap<>();
        return result;
    }



    private boolean convertResponseToBoolean(DocWriteResponse response){
        switch (response.getResult()){
            case CREATED:
            case DELETED:
            case UPDATED:
                return true;
            default:
                return false;
        }
    }

    public boolean delete(UUID clusterId, String index, String docId) throws IOException {
        DeleteResponse deleteResponse = jdbcRepository.deleteById(clusterId, index, docId);
        return convertResponseToBoolean(deleteResponse);
    }

    public boolean update(UUID clusterId, String index, String id, JdbcUpdateRequest jdbcRequest) throws IOException {
        Map<String, Object> jsonMap = new HashMap<>();
        covertUpdateRequestToMap(jdbcRequest, jsonMap);
        UpdateResponse updateResponse = jdbcRepository.save(clusterId, index, id, jsonMap);
        return convertResponseToBoolean(updateResponse);
    }

    /**
     * JDBC 도큐먼트를 다운로드를 하기 위하여 Jdbc 이름은 리스트 형태, Jdbc 풀 내용을 Json String 형태로 받는다.
     *
     * @param clusterId : 현재 사용되는 elasticsearch clusterId
     * @param index: 조회할 인덱스
     * @param sb        : 저장된 Jdbc의 내용이 Json String으로 채워짐
     * @return
     */
    @Override
    public Map<String, Object> getJdbcInfoList(UUID clusterId, String index, StringBuffer sb) {
        return jdbcRepository.getDocuments(clusterId, index, sb);
    }

    private void covertUpdateRequestToMap(JdbcUpdateRequest jdbcRequest, Map<String, Object> jsonMap){
        String id = jdbcRequest.getId();
        String name = jdbcRequest.getName();
        String driver = jdbcRequest.getDriver();
        String user = jdbcRequest.getUser();
        String password = jdbcRequest.getPassword();
        String url = jdbcRequest.getUrl();

        if(!StringUtils.isNullOrEmpty(id)) jsonMap.put("id", id);
        if(!StringUtils.isNullOrEmpty(name)) jsonMap.put("name", name);
        if(!StringUtils.isNullOrEmpty(driver)) jsonMap.put("driver", driver);
        if(!StringUtils.isNullOrEmpty(user)) jsonMap.put("user", user);
        if(!StringUtils.isNullOrEmpty(password)) jsonMap.put("password", password);
        if(!StringUtils.isNullOrEmpty(url)) jsonMap.put("url", url);
    }


    
}
