package com.danawa.dsearch.server.dictionary;

import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.dictionary.entity.DictionarySetting;
import com.danawa.dsearch.server.dictionary.service.DictionaryService;
import com.danawa.dsearch.server.indices.entity.DocumentPagination;
import com.danawa.dsearch.server.indices.service.IndicesService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class FakeDictionaryService extends DictionaryService {
    public FakeDictionaryService(String dictionaryIndex, String dictionaryApplyIndex, IndicesService indicesService, ClusterService clusterService, ElasticsearchFactory elasticsearchFactory) {
        super(dictionaryIndex, dictionaryApplyIndex, indicesService, clusterService, elasticsearchFactory);
    }

    public List<DictionarySetting> getAnalysisPluginSettings(UUID clusterId) throws IOException {
        List<DictionarySetting> settings = new ArrayList<>();
        return settings;
    }

    public DocumentPagination documentPagination(UUID clusterId, String type, long pageNum, long rowSize, boolean isMatch, String searchColumns, String value) throws IOException {
        return new DocumentPagination();
    }

}
