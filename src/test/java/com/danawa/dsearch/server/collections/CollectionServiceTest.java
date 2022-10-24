//package com.danawa.dsearch.server.collections;
//
//import com.danawa.dsearch.server.clusters.service.ClusterService;
//import com.danawa.dsearch.server.collections.entity.Collection;
//import com.danawa.dsearch.server.collections.service.CollectionService;
//import com.danawa.dsearch.server.collections.service.IndexingJobManager;
//import com.danawa.dsearch.server.collections.service.IndexingJobService;
//import com.danawa.dsearch.server.config.ElasticsearchFactory;
//import com.danawa.dsearch.server.excpetions.CronParseException;
//import com.danawa.dsearch.server.indices.service.IndicesService;
//import org.apache.commons.lang.NullArgumentException;
//import org.elasticsearch.client.RestHighLevelClient;
//import org.hamcrest.Matchers;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.DisplayName;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.Mock;
//import org.mockito.junit.jupiter.MockitoExtension;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.UUID;
//
//import static org.hamcrest.Matchers.greaterThan;
//import static org.mockito.BDDMockito.given;
//import static org.mockito.Mockito.mock;
//
//@ExtendWith(MockitoExtension.class)
//public class CollectionServiceTest {
//    private CollectionService collectionService;
//    @Mock
//    private IndexingJobService indexingJobService;
//    @Mock
//    private ClusterService clusterService;
//    @Mock
//    private ElasticsearchFactory elasticsearchFactory;
//    @Mock
//    private IndicesService indicesService;
//    @Mock
//    private IndexingJobManager indexingJobManager;
//
//    String collectionIndex = "collection";
//    String indexSuffixA = "-a";
//    String indexSuffixB = "-b";
//
//    @BeforeEach
//    public void setup(){
//        // @PostConstruct를 사용하지 않기 위해 Fake형태로 바꾸어서 사용
////        this.collectionService = new FakeCollectionService(indexingJobService, clusterService, collectionIndex, indexSuffixA, indexSuffixB, elasticsearchFactory, indicesService, indexingJobManager);
////        this.collectionService = new CollectionService(indexingJobService, clusterService, collectionIndex, indexSuffixA, indexSuffixB, elasticsearchFactory, indicesService, indexingJobManager);
//    }
//
//    @Test
//    @DisplayName("컬렉션 리스트를 찾아 반환 성공")
//    public void find_all_success() throws IOException {
//        // given
//        UUID clusterId = UUID.randomUUID();
//
//        // when
//        // 복잡하므로 Fake형태에서 가져와서 흉내만 냄
//        List<Collection> result = collectionService.findAll(clusterId);
//
//        // then
//        Assertions.assertEquals(0, result.size());
//    }
//
//    @Test
//    @DisplayName("findAll 수행 시 clusterId 가 null 일 경우 실패")
//    public void find_all_fail_when_clusterId_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            List<Collection> result = collectionService.findAll(null);
//        });
//    }
//
//    @Test
//    @DisplayName("컬렉션 ID로 컬렉션 문서 가져오기 성공")
//    public void find_by_id_success() throws IOException {
//        // given
//        UUID clusterId = UUID.randomUUID();
//        String id = "id";
//
//        // when
//        Collection collection = collectionService.findById(clusterId, id);
//
//        //then
//        Assertions.assertNotNull(collection);
//    }
//
//    @Test
//    @DisplayName("컬렉션ID로 컬렉션 문서 가져오기 수행 시 clusterId 가 null 일 경우 실패")
//    public void find_by_id_fail_when_clusterId_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "id";
//            Collection collection = collectionService.findById(null, id);
//        });
//    }
//    @Test
//    @DisplayName("컬렉션ID로 컬렉션 문서 가져오기 수행 시 id 가 null 일 경우 실패")
//    public void find_by_id_fail_when_id_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "id";
//            Collection collection = collectionService.findById(clusterId, null);
//        });
//    }
//    @Test
//    @DisplayName("컬렉션ID로 컬렉션 문서 가져오기 수행 시 id 가 빈 문자열 일 경우 실패")
//    public void find_by_id_fail_when_id_is_0_length() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "";
//            Collection collection = collectionService.findById(clusterId, id);
//        });
//    }
//
//    @Test
//    @DisplayName("컬렉션ID로 컬렉션 문서 삭제 수행 시 clusterId 가 null 일 경우 실패")
//    public void delete_by_id_fail_when_clusterId_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "id";
//            collectionService.deleteById(null, id);
//        });
//    }
//    @Test
//    @DisplayName("컬렉션ID로 컬렉션 문서 삭제 수행 시 id 가 null 일 경우 실패")
//    public void delete_by_id_fail_when_id_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "id";
//            collectionService.deleteById(clusterId, null);
//        });
//    }
//    @Test
//    @DisplayName("컬렉션ID로 컬렉션 문서 삭제 수행 시 id 가 빈 문자열 일 경우 실패")
//    public void delete_by_id_fail_when_id_is_0_length() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "";
//            collectionService.deleteById(clusterId, id);
//        });
//    }
//
//    @Test
//    @DisplayName("컬렉션 ID로 컬렉션 문서 내용 수정 시 clusterId 가 null 일 경우 실패")
//    public void editSource_fail_when_clusterId_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "id";
//            Collection collection = new Collection();
//            collectionService.editSource(null, id, collection);
//        });
//    }
//    @Test
//    @DisplayName("컬렉션 ID로 컬렉션 문서 내용 수정 시 id 가 null 일 경우 실패")
//    public void editSource_fail_when_id_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "id";
//            Collection collection = new Collection();
//            collectionService.editSource(clusterId, null, collection);
//        });
//    }
//    @Test
//    @DisplayName("컬렉션 ID로 컬렉션 문서 내용 수정 시 id 가 빈 문자열 일 경우 실패")
//    public void editSource_fail_when_id_is_0_length() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "";
//            Collection collection = new Collection();
//            collectionService.editSource(clusterId, id, collection);
//        });
//    }
//    @Test
//    @DisplayName("컬렉션 ID로 컬렉션 문서 내용 수정 시 collection 이 null 일 경우 실패")
//    public void editSource_fail_when_collection_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "id";
//            Collection collection = new Collection();
//            collectionService.editSource(clusterId, id, null);
//        });
//    }
//
//
//    @Test
//    @DisplayName("컬렉션에 등록된 스케줄 등록/수정 수행 시 clusterId 가 null 일 경우 실패")
//    public void editSchedule_fail_when_clusterId_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "id";
//            Collection collection = new Collection();
//            collectionService.editSchedule(null, id, collection);
//        });
//    }
//    @Test
//    @DisplayName("컬렉션에 등록된 스케줄 등록/수정 수행 시 id 가 null 일 경우 실패")
//    public void editSchedule_fail_when_id_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "id";
//            Collection collection = new Collection();
//            collectionService.editSchedule(clusterId, null, collection);
//        });
//    }
//    @Test
//    @DisplayName("컬렉션에 등록된 스케줄 등록/수정 수행 시 id 가 빈 문자열 일 경우 실패")
//    public void editSchedule_fail_when_id_is_0_length() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "";
//            Collection collection = new Collection();
//            collectionService.editSchedule(clusterId, id, collection);
//        });
//    }
//    @Test
//    @DisplayName("컬렉션에 등록된 스케줄 등록/수정 수행 시 collection 이 null 일 경우 실패")
//    public void editSchedule_fail_when_collection_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "id";
//            Collection collection = new Collection();
//            collectionService.editSchedule(clusterId, id, null);
//        });
//    }
//
//    @Test
//    @DisplayName("컬렉션 이름으로 컬렉션 찾기 성공")
//    public void find_by_name_success() throws IOException {
//        UUID clusterId = UUID.randomUUID();
//        String name = "name";
//        Collection collection = collectionService.findByName(clusterId, name);
//        Assertions.assertNotNull(collection);
//    }
//
//    @Test
//    @DisplayName("컬렉션 이름으로 컬렉션 찾기 시 clusterId 가 null 일 경우 실패")
//    public void find_by_name_fail_when_clusterId_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String name = "name";
//            Collection collection = collectionService.findByName(null, name);
//        });
//    }
//
//    @Test
//    @DisplayName("컬렉션 이름으로 컬렉션 찾기 시 name 가 null 일 경우 실패")
//    public void find_by_name_fail_when_id_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String name = "name";
//            Collection collection = collectionService.findByName(clusterId, null);
//        });
//    }
//    @Test
//    @DisplayName("컬렉션 이름으로 컬렉션 찾기 이름이 빈 문자열 일 경우 실패")
//    public void find_by_name_fail_when_id_is_0_length() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String name = "";
//            Collection collection = collectionService.findByName(clusterId, name);
//        });
//    }
//
////    @Test
////    @DisplayName("한 클러스터 내의 등록된 컬렉션 크론잡 스케줄 전체 삭제 시 clusterId 가 null 일 경우 실패")
////    public void remove_all_schedule_fail_when_clusterId_null() throws IOException {
////        Assertions.assertThrows(NullArgumentException.class, () -> {
////            UUID clusterId = UUID.randomUUID();
////            collectionService.removeAllSchedule(null);
////        });
////    }
////
////    @Test
////    @DisplayName("한 클러스터 내의 등록된 컬렉션 크론잡 스케줄 전체 삭제 시 clusterId 가 null 일 경우 실패")
////    public void register_all_schedule_fail_when_clusterId_null() throws IOException {
////        Assertions.assertThrows(NullArgumentException.class, () -> {
////            UUID clusterId = UUID.randomUUID();
////            collectionService.registerAllSchedule(null);
////        });
////    }
////
////    @Test
////    @DisplayName("컬렉션의 등록된 크론잡 스케줄 삭제 시 clusterId 가 null 일 경우 실패")
////    public void remove_schedule_fail_when_clusterId_null() throws IOException {
////        Assertions.assertThrows(NullArgumentException.class, () -> {
////            UUID clusterId = UUID.randomUUID();
////            String collectionId = "";
////            String crons = "* * * * *";
////            collectionService.removeSchedule(null, collectionId, crons);
////        });
////    }
////
////    @Test
////    @DisplayName("컬렉션의 등록된 크론잡 스케줄 삭제 시 collectionId 가 null 일 경우 실패")
////    public void remove_schedule_fail_when_collectionId_is_null() throws IOException {
////        Assertions.assertThrows(NullArgumentException.class, () -> {
////            UUID clusterId = UUID.randomUUID();
////            String collectionId = "";
////            String crons = "* * * * *";
////            collectionService.removeSchedule(clusterId, null, crons);
////        });
////    }
////
////    @Test
////    @DisplayName("컬렉션의 등록된 크론잡 스케줄 삭제 시 collectionId 가 빈 문자열 일 경우 실패")
////    public void remove_schedule_fail_when_collectionId_is_0_length() throws IOException {
////        Assertions.assertThrows(NullArgumentException.class, () -> {
////            UUID clusterId = UUID.randomUUID();
////            String collectionId = "";
////            String crons = "* * * * *";
////            collectionService.removeSchedule(clusterId, collectionId, crons);
////        });
////    }
////
////    @Test
////    @DisplayName("컬렉션의 등록된 크론잡 스케줄 삭제 시 clusterId 가 null 일 경우 실패")
////    public void remove_schedule_fail_when_crons_param_is_invaild() throws IOException {
////        Assertions.assertThrows(CronParseException.class, () -> {
////            UUID clusterId = UUID.randomUUID();
////            String collectionId = "id";
////            String crons = "* * * *";
////            collectionService.removeSchedule(clusterId, collectionId, crons);
////        });
////    }
////
////    @Test
////    @DisplayName("한 클러스터 내의 전체 컬렉션 크론잡 스케줄 삭제 확인")
////    public void remove_schedule_success() throws CronParseException {
////        UUID clusterId = UUID.randomUUID();
////        String collectionId = "id";
////        String crons = "30 18 * * *";
////        String key = String.format("%s-%s-%s", clusterId, collectionId, crons);
////        collectionService.registerCronJob(clusterId, collectionId, crons);
////        System.out.println(key + " : " + collectionService.isAliveSchedule(key));
////        collectionService.removeSchedule(clusterId, collectionId, crons);
////        System.out.println(key + " : " + collectionService.isAliveSchedule(key));
////    }
//
//    @Test
//    @DisplayName("컬렉션 데이터 다운로드 성공")
//    public void download_success() throws IOException {
//        UUID clusterId = UUID.randomUUID();
//        Map<String, Object> message = new HashMap<>();
//        String result = collectionService.download(clusterId, message);
//        Assertions.assertTrue(greaterThan(0).matches(result.length()));
//        Assertions.assertFalse(message.isEmpty());
//    }
//
//    @Test
//    @DisplayName("컬렉션 데이터 다운로드 실패, clusterId 가 null 일 경우")
//    public void download_fail_clusterId_is_null() throws IOException {
//        UUID clusterId = UUID.randomUUID();
//        Map<String, Object> message = new HashMap<>();
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            String result = collectionService.download(null, message);
//        });
//    }
//
//    @Test
//    @DisplayName("컬렉션 데이터 다운로드 실패, message 가 null 일 경우")
//    public void download_fail_message_is_null() throws IOException {
//        UUID clusterId = UUID.randomUUID();
//        Map<String, Object> message = new HashMap<>();
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            String result = collectionService.download(clusterId, null);
//        });
//    }
//
//}
