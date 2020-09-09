package com.danawa.dsearch.server;

import com.danawa.dsearch.server.entity.Cluster;
import com.danawa.dsearch.server.repository.ClusterRepository;
import com.danawa.dsearch.server.services.ClusterService;
import org.assertj.core.api.Assertions;
import org.hibernate.annotations.CreationTimestamp;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.aspectj.EnableSpringConfigured;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.CurrentSecurityContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import javax.persistence.Column;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;


@SpringBootTest
@RunWith(SpringRunner.class)
public class H2Test {

    @Autowired
    private ClusterRepository clusterRepository;

//    @After
//    public void deleteCluster(){
//        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-4466554401234");
//        clusterRepository.deleteById(uuid);
//    }

    @Test
    public void h2Test(){
        LocalDateTime createDate = LocalDateTime.now();
        LocalDateTime updateDate = LocalDateTime.now();
        String scheme = "http";
        String host = "localhost";
        int port = 9200;
        String name = "네임" + Math.round(Math.random() * 10)  ;

        Cluster cluster = new Cluster();
        cluster.setUpdateDate(updateDate);
        cluster.setCreateDate(createDate);
        cluster.setScheme(scheme);
        cluster.setName(name);
        cluster.setHost(host);
        cluster.setPort(port);
        clusterRepository.save(cluster);
        List<Cluster> clusterList = clusterRepository.findByHostAndPort(host, port);
        cluster = clusterList.get(0);
        Assertions.assertThat(clusterList.size()).isGreaterThan(0);
        Assertions.assertThat(cluster.getHost()).isEqualTo(host);
        Assertions.assertThat(cluster.getPort()).isEqualTo(port);
        clusterRepository.deleteById(cluster.getId());
    }
}
