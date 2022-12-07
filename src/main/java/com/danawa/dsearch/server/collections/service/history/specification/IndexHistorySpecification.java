package com.danawa.dsearch.server.collections.service.history.specification;

import com.danawa.dsearch.server.collections.service.history.entity.IndexHistory;
import org.springframework.data.jpa.domain.Specification;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Predicate;
import java.util.UUID;

public class IndexHistorySpecification {
    private static String makeIndexA(String collectionName){
        String indexA = collectionName +  "-a";
        return indexA;
    }

    private static String makeIndexB(String collectionName){
        String indexB = collectionName +  "-b";
        return indexB;
    }

    public static Specification<IndexHistory> whereExactCollectionName(UUID clusterId, String collectionName){
        return equalsClusterId(clusterId).and(equalsCollectionName(collectionName));
    }

    public static Specification<IndexHistory> whereExactJobType(UUID clusterId, String jobType){
        return equalsClusterId(clusterId).and(equalsJobType(jobType));
    }

    public static Specification<IndexHistory> whereExactCollectionNameAndJobType(UUID clusterId, String jobType, String collectionName){
        return equalsClusterId(clusterId).and(equalsJobType(jobType)).and(equalsCollectionName(collectionName));
    }

    public static Specification<IndexHistory> equalsClusterId(UUID clusterId) {
        return new Specification<IndexHistory>() {
            @Override
            public Predicate toPredicate(Root<IndexHistory> root, CriteriaQuery<?> cq, CriteriaBuilder builder) {
                return builder.equal(root.get("clusterId"), clusterId);
            }
        };
    }
    public static Specification<IndexHistory> equalsJobType(String jobType) {
        return new Specification<IndexHistory>() {
            @Override
            public Predicate toPredicate(Root<IndexHistory> root, CriteriaQuery<?> cq, CriteriaBuilder builder) {
                return builder.equal(root.get("jobType"), jobType);
            }
        };
    }

    public static Specification<IndexHistory> equalsCollectionName(String collectionName) {
        return new Specification<IndexHistory>() {
            @Override
            public Predicate toPredicate(Root<IndexHistory> root, CriteriaQuery<?> cq, CriteriaBuilder builder) {
                return builder.or(builder.equal(root.get("index"), makeIndexA(collectionName)), builder.equal(root.get("index"), makeIndexB(collectionName)));
            }
        };
    }
}
