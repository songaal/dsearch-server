package com.danawa.dsearch.server.collections.service.indexer;

import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexerStatus;
import com.danawa.dsearch.server.collections.entity.IndexingInfo;
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

    private final IndexJobManager indexJobManager;

    public IndexerClientImpl(IndexJobManager indexJobManager){
        this.indexJobManager = indexJobManager;
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(10 * 1000);
        factory.setReadTimeout(10 * 1000);
        restTemplate = new RestTemplate(factory);
    }

    public IndexerStatus getStatus(IndexingInfo indexingInfo){
        boolean isExtIndexer = indexingInfo.getCollection().isExtIndexer();

        if (isExtIndexer) {
            return getStatusForExternal(indexingInfo);
        } else {
            return getStatusForInternal(indexingInfo);
        }
    }

    private IndexerStatus getStatusForExternal(IndexingInfo indexingInfo){
        String scheme = indexingInfo.getScheme();
        String host = indexingInfo.getHost();
        int port = indexingInfo.getPort();
        String jobId = indexingInfo.getIndexingJobId();

        URI url = URI.create(String.format("%s://%s:%d/async/status?id=%s", scheme, host, port, jobId));
        ResponseEntity<Map> responseEntity = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(new HashMap<>()), Map.class);
        Map<String, Object> body = responseEntity.getBody();

        // Null pointer Exception
        if (body.get("status") != null) return IndexerStatus.changeToStatus((String) body.get("status"));
        return IndexerStatus.UNKNOWN;
    }

    private IndexerStatus getStatusForInternal(IndexingInfo indexingInfo){
        String jobId = indexingInfo.getIndexingJobId();
        Job job = indexJobManager.status(UUID.fromString(jobId));
        if (job != null) return IndexerStatus.changeToStatus(job.getStatus());

        return IndexerStatus.UNKNOWN;
    }

    public void deleteJob(IndexingInfo indexingInfo){
        boolean isExtIndexer = indexingInfo.getCollection().isExtIndexer();

        if (isExtIndexer) {
            deleteJobForExternal(indexingInfo);
        } else {
            deleteJobForInternal(indexingInfo);
        }
    }
    private void deleteJobForExternal(IndexingInfo indexingInfo){
        URI deleteUrl = URI.create(String.format("http://%s:%d/async/%s", indexingInfo.getHost(), indexingInfo.getPort(), indexingInfo.getIndexingJobId()));
        ResponseEntity<Map> responseEntity = restTemplate.exchange(deleteUrl, HttpMethod.DELETE, new HttpEntity<>(new HashMap<>()), Map.class);
        Map<String, Object> body = responseEntity.getBody();
        logger.debug("deleteJob response ==> index={}, status={}", indexingInfo.getIndex(), body.get("status"));
    }

    private void deleteJobForInternal(IndexingInfo indexingInfo){
        String jobId = indexingInfo.getIndexingJobId();
        indexJobManager.remove(UUID.fromString(jobId));
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
        Job job = indexJobManager.start(action, body);
        return job.getId().toString();
    }
    public void stopJob(IndexingInfo status) throws URISyntaxException {
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
        indexJobManager.stop(UUID.fromString(jobId));
    }

    public void startGroupJob(IndexingInfo status, Collection collection, String groupSeq) throws URISyntaxException {
        boolean isExtIndexer = collection.isExtIndexer();
        String jobId = status.getIndexingJobId();
        Collection.Launcher launcher = collection.getLauncher();
        String scheme = status.getScheme();
        String host = launcher.getHost();
        int port = launcher.getPort();

        if (isExtIndexer) {
            logger.info(">>>>> Ext call sub_start: id: {}, groupSeq: {}", jobId, groupSeq);
            startGroupJobForExternal(scheme, host, port, jobId, groupSeq);
        } else {
            logger.info(">>>>> Local call sub_start: id: {}, groupSeq: {}", jobId, groupSeq);
            startGroupJobForInternal(jobId, groupSeq);
        }
    }

    private void startGroupJobForExternal(String scheme, String host, int port, String jobId, String groupSeq ) throws URISyntaxException {
        restTemplate.exchange(new URI(String.format("%s://%s:%d/async/%s/sub_start?groupSeq=%s", scheme, host, port, jobId, groupSeq)),
                HttpMethod.PUT,
                new HttpEntity(new HashMap<>()),
                String.class
        );
    }

    private void startGroupJobForInternal(String jobId, String groupSeq) {
        Job job = indexJobManager.status(UUID.fromString(jobId));
        job.getGroupSeq().add(Integer.parseInt(groupSeq));
    }
}
