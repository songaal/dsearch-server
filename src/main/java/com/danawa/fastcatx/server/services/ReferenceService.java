package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.entity.services.ReferenceEntity;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
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
import java.util.List;

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


    public List<ReferenceEntity> findAll() {
        return null;
    }
}
