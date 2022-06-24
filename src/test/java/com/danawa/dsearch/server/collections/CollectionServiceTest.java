package com.danawa.dsearch.server.collections;

import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.service.CollectionService;
import com.danawa.dsearch.server.collections.service.IndexingJobManager;
import com.danawa.dsearch.server.collections.service.IndexingJobService;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.excpetions.CronParseException;
import com.danawa.dsearch.server.indices.service.IndicesService;
import com.danawa.dsearch.server.rankingtuning.FakeRankingTuningService;
import org.apache.commons.lang.NullArgumentException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.greaterThan;

@ExtendWith(MockitoExtension.class)
public class CollectionServiceTest {
    private CollectionService collectionService;
    @Mock
    private IndexingJobService indexingJobService;
    @Mock
    private ClusterService clusterService;
    @Mock
    ElasticsearchFactory elasticsearchFactory;
    @Mock
    IndicesService indicesService;
    @Mock
    IndexingJobManager indexingJobManager;

    String collectionIndex = "collection";
    String indexSuffixA = "-a";
    String indexSuffixB = "-b";

    @BeforeEach
    public void setup(){
        this.collectionService = new FakeCollectionService(indexingJobService, clusterService, collectionIndex, indexSuffixA, indexSuffixB, elasticsearchFactory, indicesService, indexingJobManager);
    }

    @Test
    public void find_all_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        List<Collection> result = collectionService.findAll(clusterId);
        Assertions.assertEquals(0, result.size());
    }

    @Test
    @DisplayName("findAll 수행 시 clusterId 가 null 일 경우 실패")
    public void find_all_fail_when_clusterId_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            List<Collection> result = collectionService.findAll(null);
        });
    }

    @Test
    public void find_by_id_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String id = "id";
        Collection collection = collectionService.findById(clusterId, id);
        Assertions.assertNotNull(collection);
    }

    @Test
    @DisplayName("findById 수행 시 clusterId 가 null 일 경우 실패")
    public void find_by_id_fail_when_clusterId_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "id";
            Collection collection = collectionService.findById(null, id);
        });
    }
    @Test
    @DisplayName("findById 수행 시 id 가 null 일 경우 실패")
    public void find_by_id_fail_when_id_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "id";
            Collection collection = collectionService.findById(clusterId, null);
        });
    }
    @Test
    @DisplayName("findById 수행 시 id 가 빈 문자열 일 경우 실패")
    public void find_by_id_fail_when_id_is_0_length() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "";
            Collection collection = collectionService.findById(clusterId, id);
        });
    }

    @Test
    @DisplayName("deleteById 수행 시 clusterId 가 null 일 경우 실패")
    public void delete_by_id_fail_when_clusterId_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "id";
            collectionService.deleteById(null, id);
        });
    }
    @Test
    @DisplayName("deleteById 수행 시 id 가 null 일 경우 실패")
    public void delete_by_id_fail_when_id_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "id";
            collectionService.deleteById(clusterId, null);
        });
    }
    @Test
    @DisplayName("deleteById 수행 시 id 가 빈 문자열 일 경우 실패")
    public void delete_by_id_fail_when_id_is_0_length() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "";
            collectionService.deleteById(clusterId, id);
        });
    }

    @Test
    @DisplayName("editSource 수행 시 clusterId 가 null 일 경우 실패")
    public void editSource_fail_when_clusterId_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "id";
            Collection collection = new Collection();
            collectionService.editSource(null, id, collection);
        });
    }
    @Test
    @DisplayName("editSource 수행 시 id 가 null 일 경우 실패")
    public void editSource_fail_when_id_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "id";
            Collection collection = new Collection();
            collectionService.editSource(clusterId, null, collection);
        });
    }
    @Test
    @DisplayName("editSource 수행 시 id 가 빈 문자열 일 경우 실패")
    public void editSource_fail_when_id_is_0_length() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "";
            Collection collection = new Collection();
            collectionService.editSource(clusterId, id, collection);
        });
    }
    @Test
    @DisplayName("editSource 수행 시 collection 이 null 일 경우 실패")
    public void editSource_fail_when_collection_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "id";
            Collection collection = new Collection();
            collectionService.editSource(clusterId, id, null);
        });
    }


    @Test
    @DisplayName("editSchedule 수행 시 clusterId 가 null 일 경우 실패")
    public void editSchedule_fail_when_clusterId_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "id";
            Collection collection = new Collection();
            collectionService.editSchedule(null, id, collection);
        });
    }
    @Test
    @DisplayName("editSchedule 수행 시 id 가 null 일 경우 실패")
    public void editSchedule_fail_when_id_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "id";
            Collection collection = new Collection();
            collectionService.editSchedule(clusterId, null, collection);
        });
    }
    @Test
    @DisplayName("editSchedule 수행 시 id 가 빈 문자열 일 경우 실패")
    public void editSchedule_fail_when_id_is_0_length() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "";
            Collection collection = new Collection();
            collectionService.editSchedule(clusterId, id, collection);
        });
    }
    @Test
    @DisplayName("editSchedule 수행 시 collection 이 null 일 경우 실패")
    public void editSchedule_fail_when_collection_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "id";
            Collection collection = new Collection();
            collectionService.editSchedule(clusterId, id, null);
        });
    }

    @Test
    public void find_by_name_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String name = "name";
        Collection collection = collectionService.findByName(clusterId, name);
        Assertions.assertNotNull(collection);
    }

    @Test
    @DisplayName("findByName 수행 시 clusterId 가 null 일 경우 실패")
    public void find_by_name_fail_when_clusterId_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            Collection collection = collectionService.findByName(null, name);
        });
    }

    @Test
    @DisplayName("findByName 수행 시 name 가 null 일 경우 실패")
    public void find_by_name_fail_when_id_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            Collection collection = collectionService.findByName(clusterId, null);
        });
    }
    @Test
    @DisplayName("findByName 수행 시 name 가 빈 문자열 일 경우 실패")
    public void find_by_name_fail_when_id_is_0_length() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "";
            Collection collection = collectionService.findByName(clusterId, name);
        });
    }

    @Test
    @DisplayName("removeAllSchedule 수행 시 clusterId 가 null 일 경우 실패")
    public void remove_all_schedule_fail_when_clusterId_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            collectionService.removeAllSchedule(null);
        });
    }

    @Test
    @DisplayName("registerAllSchedule 수행 시 clusterId 가 null 일 경우 실패")
    public void register_all_schedule_fail_when_clusterId_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            collectionService.registerAllSchedule(null);
        });
    }

    @Test
    @DisplayName("removeSchedule 수행 시 clusterId 가 null 일 경우 실패")
    public void remove_schedule_fail_when_clusterId_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String collectionId = "";
            String crons = "* * * * *";
            collectionService.removeSchedule(null, collectionId, crons);
        });
    }

    @Test
    @DisplayName("removeSchedule 수행 시 collectionId 가 null 일 경우 실패")
    public void remove_schedule_fail_when_collectionId_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String collectionId = "";
            String crons = "* * * * *";
            collectionService.removeSchedule(clusterId, null, crons);
        });
    }

    @Test
    @DisplayName("removeSchedule 수행 시 collectionId 가 빈 문자열 일 경우 실패")
    public void remove_schedule_fail_when_collectionId_is_0_length() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String collectionId = "";
            String crons = "* * * * *";
            collectionService.removeSchedule(clusterId, collectionId, crons);
        });
    }

    @Test
    @DisplayName("removeSchedule 수행 시 clusterId 가 null 일 경우 실패")
    public void remove_schedule_fail_when_crons_param_is_invaild() throws IOException {
        Assertions.assertThrows(CronParseException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String collectionId = "id";
            String crons = "* * * *";
            collectionService.removeSchedule(clusterId, collectionId, crons);
        });
    }

    @Test
    @DisplayName("스케줄 삭제 확인")
    public void remove_schedule_success() throws CronParseException, InterruptedException {
        UUID clusterId = UUID.randomUUID();
        String collectionId = "id";
        String crons = "30 18 * * *";
        String key = String.format("%s-%s-%s", clusterId, collectionId, crons);
        collectionService.registerOneSchedule(clusterId, collectionId, crons);
        System.out.println(key + " : " + collectionService.isAliveSchedule(key));
        collectionService.removeSchedule(clusterId, collectionId, crons);
        System.out.println(key + " : " + collectionService.isAliveSchedule(key));
    }

    @Test
    @DisplayName("download 성공")
    public void download_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        Map<String, Object> message = new HashMap<>();
        String result = collectionService.download(clusterId, message);
        Assertions.assertTrue(greaterThan(0).matches(result.length()));
        Assertions.assertFalse(message.isEmpty());
    }

    @Test
    @DisplayName("download 실패, clusterId 가 null 일 경우")
    public void download_fail_clusterId_is_null() throws IOException {
        UUID clusterId = UUID.randomUUID();
        Map<String, Object> message = new HashMap<>();
        Assertions.assertThrows(NullArgumentException.class, () -> {
            String result = collectionService.download(null, message);
        });
    }

    @Test
    @DisplayName("download 실패, message 가 null 일 경우")
    public void download_fail_message_is_null() throws IOException {
        UUID clusterId = UUID.randomUUID();
        Map<String, Object> message = new HashMap<>();
        Assertions.assertThrows(NullArgumentException.class, () -> {
            String result = collectionService.download(clusterId, null);
        });
    }

}
