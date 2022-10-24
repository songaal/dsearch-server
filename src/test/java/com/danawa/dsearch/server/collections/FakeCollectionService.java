//package com.danawa.dsearch.server.collections;
//
//import com.danawa.dsearch.server.clusters.service.ClusterService;
//import com.danawa.dsearch.server.collections.entity.Collection;
//import com.danawa.dsearch.server.collections.service.CollectionService;
//import com.danawa.dsearch.server.collections.service.IndexingJobManager;
//import com.danawa.dsearch.server.collections.service.IndexingJobService;
//import com.danawa.dsearch.server.config.ElasticsearchFactory;
//import com.danawa.dsearch.server.indices.service.IndicesService;
//import org.apache.commons.lang.NullArgumentException;
//
//import java.io.IOException;
//import java.util.*;
//
//public class FakeCollectionService extends CollectionService {
//    public FakeCollectionService(IndexingJobService indexingJobService, ClusterService clusterService, String collectionIndex, String indexSuffixA, String indexSuffixB, ElasticsearchFactory elasticsearchFactory, IndicesService indicesService, IndexingJobManager indexingJobManager) {
//        super(indexingJobService, clusterService, collectionIndex, indexSuffixA, indexSuffixB, elasticsearchFactory, indicesService, indexingJobManager);
//    }
//
//    /**
//     * 재정의
//     * */
//    public void init() { }
//
//    public List<Collection> findAll(UUID clusterId) throws IOException {
//        if(clusterId == null) throw new NullArgumentException("");
//
//        List<Collection> result = new ArrayList<Collection>();
//        return result;
//    }
//
//    public Collection findById(UUID clusterId, String id) throws IOException{
//        if(clusterId == null || id == null || id.equals("")) throw new NullArgumentException("");
//
//        return new Collection();
//    }
//
////    public Collection findByName(UUID clusterId, String name) throws IOException{
////        if(clusterId == null || name == null || name.equals("")) throw new NullArgumentException("");
////
////        return new Collection();
////    }
////
////    public void deleteById(UUID clusterId, String id) throws IOException{
////        if(clusterId == null || id == null || id.equals("")) throw new NullArgumentException("");
////    }
////
////    public String download(UUID clusterId, Map<String, Object> message){
////        if(clusterId == null || message == null ) throw new NullArgumentException("");
////        Map<String, Object> collection = new HashMap<>();
////        message.put("collection", collection);
////        return "download";
////    }
//}
