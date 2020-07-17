package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import com.danawa.fastcatx.server.entity.Collection;
import com.danawa.fastcatx.server.excpetions.IndexingJobFailureException;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class IndexingJobService {

    private static Logger logger = LoggerFactory.getLogger(IndexingJobService.class);
    private final ElasticsearchFactory elasticsearchFactory;
    private final CollectionService collectionService;

    private final String exclude = "search*";

    public IndexingJobService(ElasticsearchFactory elasticsearchFactory, CollectionService collectionService) {
        this.elasticsearchFactory = elasticsearchFactory;
        this.collectionService = collectionService;
    }

    /**
     * 소스를 읽어들여 ES 색인에 입력하는 작업.
     * indexer를 외부 프로세스로 실행한다.
     * */
    public synchronized void indexing(UUID clusterId, String collectionId) throws IOException, IndexingJobFailureException {
        Collection collection = collectionService.findById(clusterId, collectionId);

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Collection.Index indexA = collection.getIndexA();
            Collection.Index indexB = collection.getIndexB();


            // 인덱스에 대한 alias를 확인
            String index = null;
            if (indexA.getAliases() == null && indexB.getAliases() == null) {
                index = indexA.getIndex();
            } else if (indexA.getAliases().get(collection.getBaseId()) != null) {
                index = indexB.getIndex();
            } else if (indexB.getAliases().get(collection.getBaseId()) != null) {
                index = indexA.getIndex();
            }

            // settings에 index.routing.allocation.include._exclude=search* 호출
            // refresh interval: -1로 변경. 색인도중 검색노출하지 않음. 성능향상목적.
            Map<String, Object> ready = new HashMap<>();
            ready.put("refresh_interval", "-1");
            ready.put("index.routing.allocation.include._exclude", "search*");
            client.indices().putSettings(new UpdateSettingsRequest().indices(index).settings(ready), RequestOptions.DEFAULT);

            Collection.Launcher launcher = collection.getLauncher();
            String yaml = launcher.getYaml();
            if (yaml == null || "".equals(yaml)) {
                throw new IndexingJobFailureException("empty yaml");
            }

            // 인덱서에 REST API를 호출하여 색인시작. 문서가 index node로 bulk insert되기 시작함.
            String host = launcher.getHost();
            int port = launcher.getPort();



            // 중간 중간 status 확인 (SUCCESS, ERROR)
//            ThreadPoolTaskScheduler scheduler
//            scheduler.schedule(getRunnable(), getTrigger());
//            new CronTrigger()


            // refresh interval: 1s로 다시 원복.
//            Map<String, Object> success = new HashMap<>();
//            success.put("refresh_interval", "-1");
//            success.put("index.routing.allocation.include._exclude", "search*");
//            client.indices().putSettings(new UpdateSettingsRequest().indices(index).settings(success), RequestOptions.DEFAULT);

        } catch (IndexingJobFailureException e) {
            logger.error("", e);
            throw e;
        }
    }

    /**
     * 색인 샤드의 TAG 제약을 풀고 전체 클러스터로 확장시킨다.
     * */
    public void propagate() {

    }

    /**
     * 색인을 대표이름으로 alias 하고 노출한다.
     * */
    public void expose() {

    }
}
