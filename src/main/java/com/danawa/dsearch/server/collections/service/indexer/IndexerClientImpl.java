package com.danawa.dsearch.server.collections.service.indexer;

import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexerStatus;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.config.IndexerConfig;
import com.danawa.fastcatx.indexer.IndexJobManager;
import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class IndexerClientImpl implements IndexerClient{
    /**
     *  외부 혹은 자체 인덱서와 통신 전용 객체 입니다.
     *  메서드명에 외부와 통신하는 경우 ForExternal, 내부와 통신하는 경우 ForInternal 로 작성 되어 있습니다.
     *  외부에서 호출하는 메서드는 public으로만 선언 했습니다.
     */

    private static final Logger logger = LoggerFactory.getLogger(IndexerClientImpl.class);
    private static RestTemplate restTemplate;

    private final IndexJobManager indexerJobManager;

    public IndexerClientImpl(IndexJobManager indexerJobManager){
        this.indexerJobManager = indexerJobManager;
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(10 * 1000);
        factory.setReadTimeout(10 * 1000);
        restTemplate = new RestTemplate(factory);
    }

    public IndexerStatus getStatus(IndexingStatus indexingStatus){
        boolean isExtIndexer = indexingStatus.getCollection().isExtIndexer();

        if (isExtIndexer) {
            return getStatusForExternal(indexingStatus);
        } else {
            return getStatusForInternal(indexingStatus);
        }
    }

    private IndexerStatus getStatusForExternal(IndexingStatus indexingStatus){
        String scheme = indexingStatus.getScheme();
        String host = indexingStatus.getHost();
        int port = indexingStatus.getPort();
        String jobId = indexingStatus.getIndexingJobId();

        URI url = URI.create(String.format("%s://%s:%d/async/status?id=%s", scheme, host, port, jobId));
        ResponseEntity<Map> responseEntity = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(new HashMap<>()), Map.class);
        Map<String, Object> body = responseEntity.getBody();

        // Null pointer Exception
        if (body.get("status") != null) return IndexerStatus.changeToStatus((String) body.get("status"));
        return IndexerStatus.UNKNOWN;
    }

    private IndexerStatus getStatusForInternal(IndexingStatus indexingStatus){
        String jobId = indexingStatus.getIndexingJobId();
        Job job = indexerJobManager.status(UUID.fromString(jobId));
        if (job != null) return IndexerStatus.changeToStatus(job.getStatus());

        return IndexerStatus.UNKNOWN;
    }

    public void deleteJob(IndexingStatus indexingStatus){
        boolean isExtIndexer = indexingStatus.getCollection().isExtIndexer();

        if (isExtIndexer) {
            deleteJobForExternal(indexingStatus);
        } else {
            deleteJobForInternal(indexingStatus);
        }
    }
    private void deleteJobForExternal(IndexingStatus indexingStatus){
        URI deleteUrl = URI.create(String.format("http://%s:%d/async/%s", indexingStatus.getHost(), indexingStatus.getPort(), indexingStatus.getIndexingJobId()));
        ResponseEntity<Map> responseEntity = restTemplate.exchange(deleteUrl, HttpMethod.DELETE, new HttpEntity<>(new HashMap<>()), Map.class);
        Map<String, Object> body = responseEntity.getBody();
        logger.debug("deleteJob response ==> index={}, status={}", indexingStatus.getIndex(), body.get("status"));
    }

    private void deleteJobForInternal(IndexingStatus indexingStatus){
        String jobId = indexingStatus.getIndexingJobId();
        indexerJobManager.remove(UUID.fromString(jobId));
    }

    public String startJob(Map<String, Object> body, Collection collection) throws URISyntaxException {
        Collection.Launcher launcher = collection.getLauncher();
        boolean isExtIndexer = collection.isExtIndexer();
        if(isExtIndexer){
            return startJobForExternal(body, launcher);
        }else{
            String action = IndexerConfig.ACTION.FULL_INDEX.name();
            return startJobForInternal(body, action);
        }
    }

    private String startJobForExternal(Map<String, Object> body, Collection.Launcher launcher) throws URISyntaxException {
        ResponseEntity<Map> responseEntity = restTemplate.exchange(
                new URI(String.format("%s://%s:%d/async/start", launcher.getScheme(), launcher.getHost(), launcher.getPort())),
                HttpMethod.POST,
                new HttpEntity(body),
                Map.class
        );
        if (responseEntity.getBody() == null) {
            throw new NullPointerException("Indexer Start Failed!");
        }
        return (String) responseEntity.getBody().get("id");
    }

    private String startJobForInternal(Map<String, Object> body, String action){
        Job job = indexerJobManager.start(action, body);
        return job.getId().toString();
    }
    public void stopJob(IndexingStatus status) throws URISyntaxException {
        Collection collection = status.getCollection();
        Collection.Launcher launcher = collection.getLauncher();
        String scheme = status.getScheme();
        String host = launcher.getHost();
        int port = launcher.getPort();
        String jobId = status.getIndexingJobId();
        boolean isExtIndexer = collection.isExtIndexer();

        if(isExtIndexer){
            stopJobForExternal(scheme, host, port, jobId);
        }else{
            stopJobForInternal(jobId);
        }
    }
    private void stopJobForExternal(String scheme, String host, int port, String jobId) throws URISyntaxException {
        restTemplate.exchange(new URI(String.format("%s://%s:%d/async/stop?id=%s", scheme, host, port, jobId)),
                HttpMethod.PUT,
                new HttpEntity(new HashMap<>()),
                String.class
        );
    }
    private void stopJobForInternal(String jobId){
        indexerJobManager.stop(UUID.fromString(jobId));
    }

    public void subStart(IndexingStatus status, Collection collection, String groupSeq) throws URISyntaxException {
        boolean isExtIndexer = collection.isExtIndexer();
        String jobId = status.getIndexingJobId();
        Collection.Launcher launcher = collection.getLauncher();
        String scheme = status.getScheme();
        String host = launcher.getHost();
        int port = launcher.getPort();

        if (isExtIndexer) {
            logger.info(">>>>> Ext call sub_start: id: {}, groupSeq: {}", jobId, groupSeq);
            subStartForExternal(scheme, host, port, jobId, groupSeq);
        } else {
            logger.info(">>>>> Local call sub_start: id: {}, groupSeq: {}", jobId, groupSeq);
            subStartForInternal(jobId, groupSeq);
        }
    }

    private void subStartForExternal(String scheme, String host, int port, String jobId, String groupSeq ) throws URISyntaxException {
        restTemplate.exchange(new URI(String.format("%s://%s:%d/async/%s/sub_start?groupSeq=%s", scheme, host, port, jobId, groupSeq)),
                HttpMethod.PUT,
                new HttpEntity(new HashMap<>()),
                String.class
        );
    }

    private void subStartForInternal(String jobId, String groupSeq) {
        Job job = indexerJobManager.status(UUID.fromString(jobId));
        job.getGroupSeq().add(Integer.parseInt(groupSeq));
    }
}
