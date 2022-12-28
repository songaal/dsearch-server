package com.danawa.dsearch.server.documentAnalysis.adapter;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class DocumentAnalysisElasticsearchAdapter implements DocumentAnalysisAdapter{

    private ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper;

    DocumentAnalysisElasticsearchAdapter(ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper){
        this.elasticsearchFactoryHighLevelWrapper = elasticsearchFactoryHighLevelWrapper;
    }

    @Override
    public Map<String, Object> getIndexMappings(UUID clusterId, String index) throws IOException {
        return elasticsearchFactoryHighLevelWrapper.getIndexMappings(clusterId, index);
    }

    @Override
    public List<Map<String, Object>> findAll(UUID clusterId, String index, String mergedQuery) throws IOException {
        return elasticsearchFactoryHighLevelWrapper.search(clusterId, index, mergedQuery);
    }

    @Override
    public Map<String, Object> getTerms(UUID clusterId, String index, String docId, String[] fields) throws IOException {
        Map<String, Object> result = new HashMap<>();
        TermVectorsResponse termVectorsResponse = elasticsearchFactoryHighLevelWrapper.getTermVectors(clusterId, index, docId, fields);

        List<TermVectorsResponse.TermVector> termVectorList = termVectorsResponse.getTermVectorsList();

        for(TermVectorsResponse.TermVector termVector : termVectorList){
            String fieldName = termVector.getFieldName();
            List<String> termsList = new ArrayList<>();

            for(TermVectorsResponse.TermVector.Term term: termVector.getTerms()){
                termsList.add(term.getTerm());
            }

            result.put(fieldName, String.join(", ", termsList));
        }

        return result;
    }

    @Override
    public Map<String, Object> findById(UUID clusterId, String index, String docId) throws IOException {
        return elasticsearchFactoryHighLevelWrapper.searchForOneDocument(clusterId, index, docId);
    }

}
