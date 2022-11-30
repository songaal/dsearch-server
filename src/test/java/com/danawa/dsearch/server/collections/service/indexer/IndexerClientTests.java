package com.danawa.dsearch.server.collections.service.indexer;

import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexerStatus;
import com.danawa.dsearch.server.collections.entity.IndexingInfo;
import com.danawa.dsearch.server.config.IndexerConfig;
import com.danawa.fastcatx.indexer.IndexJobManager;
import com.danawa.fastcatx.indexer.entity.Job;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
public class IndexerClientTests {

    private IndexerClient indexerClient;

    @Mock
    private IndexJobManager indexJobManager;

    @BeforeEach
    public void setup(){
        this.indexerClient = new IndexerClientImpl(this.indexJobManager);
    }

    /**
     * 외부 Indexer와 동일한 Dsearch-Indexer 라이브러리를 사용하므로 Internal만 테스트 합니다.
     */

    @Test
    @DisplayName("인덱싱 상태 가져오기 테스트")
    public void getStatusTest(){
        /**
         * 외부 Indexer와 동일한 Dsearch-Indexer 라이브러리를 사용하므로 Internal만 테스트 합니다.
         */

        // 1. 기본 데이터 셋팅
        Collection collection = new Collection();
        collection.setExtIndexer(false);
        IndexingInfo indexingInfo = new IndexingInfo();
        indexingInfo.setCollection(collection);

        String jobId = UUID.randomUUID().toString();
        indexingInfo.setIndexingJobId(jobId); // 테스트용 임시 UUID

        // 2. SUCCESS 테스트
        Job successJob = new Job();
        successJob.setStatus("SUCCESS");

        given(indexJobManager.status(UUID.fromString(jobId))).willReturn(successJob);

        IndexerStatus indexerStatus = indexerClient.getStatus(indexingInfo);
        Assertions.assertEquals(IndexerStatus.SUCCESS, indexerStatus);

        // 3. RUNNING 테스트
        Job runningJob = new Job();
        runningJob.setStatus("RUNNING");
        given(indexJobManager.status(UUID.fromString(jobId))).willReturn(runningJob);

        indexerStatus = indexerClient.getStatus(indexingInfo);
        Assertions.assertEquals(IndexerStatus.RUNNING, indexerStatus);

        // 4. STOP 테스트
        Job stopJob = new Job();
        stopJob.setStatus("STOP");
        given(indexJobManager.status(UUID.fromString(jobId))).willReturn(stopJob);

        indexerStatus = indexerClient.getStatus(indexingInfo);
        Assertions.assertEquals(IndexerStatus.STOP, indexerStatus);

        // 5. ERROR 테스트
        Job errorJob = new Job();
        errorJob.setStatus("ERROR");
        given(indexJobManager.status(UUID.fromString(jobId))).willReturn(errorJob);

        indexerStatus = indexerClient.getStatus(indexingInfo);
        Assertions.assertEquals(IndexerStatus.ERROR, indexerStatus);

        // 6. UNKNOWN 테스트
        Job unknwonJob = new Job();
        unknwonJob.setStatus("");
        given(indexJobManager.status(UUID.fromString(jobId))).willReturn(unknwonJob);

        indexerStatus = indexerClient.getStatus(indexingInfo);
        Assertions.assertEquals(IndexerStatus.UNKNOWN, indexerStatus);
    }

    @Test
    @DisplayName("인덱싱 시작 테스트")
    public void startJobTest(){
        // 1. 기본 데이터 셋팅
        Collection.Launcher launcher = new Collection.Launcher();
        Collection collection = new Collection();
        collection.setExtIndexer(false);
        collection.setLauncher(launcher);
        Map<String, Object> body = new HashMap<>();

        // 2. 테스트
        String jobId = UUID.randomUUID().toString();
        Job successJob = new Job();
        successJob.setStatus("SUCCESS");
        successJob.setId(UUID.fromString(jobId));

        given(indexJobManager.start(IndexerConfig.ACTION.FULL_INDEX.name(), body)).willReturn(successJob);

        Assertions.assertDoesNotThrow(() -> {
            String successJobId = indexerClient.startJob(body, collection);
            Assertions.assertEquals(successJobId, jobId);
        });
    }

    @Test
    @DisplayName("인덱싱 중지 테스트")
    public void stopJobTest(){
        // 1. 기본 데이터 셋팅
        Collection.Launcher launcher = new Collection.Launcher();
        Collection collection = new Collection();
        collection.setExtIndexer(false);
        collection.setLauncher(launcher);
        IndexingInfo indexingInfo = new IndexingInfo();
        indexingInfo.setCollection(collection);


        // 2. 테스트
        UUID jobId = UUID.randomUUID();
        indexingInfo.setIndexingJobId(jobId.toString());
        Assertions.assertDoesNotThrow(() -> {
            indexerClient.stopJob(indexingInfo);
        });
    }

    @Test
    @DisplayName("인덱싱 종료 테스트")
    public void deleteJobTest(){
        // 1. 기본 데이터 셋팅
        UUID jobId = UUID.randomUUID();

        Collection.Launcher launcher = new Collection.Launcher();
        Collection collection = new Collection();
        collection.setExtIndexer(false);
        collection.setLauncher(launcher);
        IndexingInfo indexingInfo = new IndexingInfo();
        indexingInfo.setCollection(collection);
        indexingInfo.setIndexingJobId(jobId.toString());

        // 2. 테스트
        Assertions.assertDoesNotThrow(() -> {
            indexerClient.deleteJob(indexingInfo);
        });
    }

    @Test
    @DisplayName("그룹 인덱싱 시작 테스트")
    public void startJobWithGroupTest(){
        /**
         * 그룹 인덱싱 시작 시에 어떻게 동작하는지 확인 필요
         */
        // 1. 기본 데이터 셋팅
        UUID jobId = UUID.randomUUID();
        Job job = new Job();
        job.setId(jobId);


        Collection.Launcher launcher = new Collection.Launcher();
        Collection collection = new Collection();
        collection.setExtIndexer(false);
        collection.setLauncher(launcher);

        IndexingInfo indexingInfo = new IndexingInfo();
        indexingInfo.setCollection(collection);
        indexingInfo.setIndexingJobId(jobId.toString());

        given(indexJobManager.status(jobId)).willReturn(job);

        // 2. 테스트
        Assertions.assertDoesNotThrow(() -> {
            indexerClient.startGroupJob(indexingInfo, collection, "0");

            Assertions.assertTrue(job.getGroupSeq().contains(0));
        });
    }
}
