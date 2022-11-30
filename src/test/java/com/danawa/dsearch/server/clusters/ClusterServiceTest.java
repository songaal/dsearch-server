package com.danawa.dsearch.server.clusters;

import com.danawa.dsearch.server.clusters.entity.Cluster;
import com.danawa.dsearch.server.clusters.dto.ClusterStatusRequest;
import com.danawa.dsearch.server.clusters.dto.ClusterStatusResponse;
import com.danawa.dsearch.server.clusters.repository.ClusterRepository;
import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.excpetions.NotFoundException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.hamcrest.MockitoHamcrest;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class ClusterServiceTest {
    @Mock
    private ClusterRepository clusterRepository ;
    private ClusterService clusterService;
    private ClusterService fakeClusterService;

    private RepositoryHelper repositoryHelper;

    @BeforeEach
    public void setup(){
        this.clusterService = new ClusterService(clusterRepository);
        this.fakeClusterService = new FakeClusterService(clusterRepository);
        this.repositoryHelper = new RepositoryHelper();
    }

    @Test
    @DisplayName("등록된 클러스터 전체를 찾는다. 그러나 한개도 없었다")
    public void find_all_clusters_but_empty(){
        // given
        given(clusterRepository.findAll()).willReturn(repositoryHelper.getAllClutersForEmpty());
        //when
        List<Cluster> clusterList =  this.clusterService.findAll();
        //then
        Assertions.assertEquals(0, clusterList.size());
    }

    @Test
    @DisplayName("등록된 클러스터 전체를 찾는다. 그러나 한개만 있었다")
    public void find_all_clusters_just_one(){
        // given
        given(clusterRepository.findAll()).willReturn(repositoryHelper.getAllClutersJustOne());
        //when
        List<Cluster> clusterList =  this.clusterService.findAll();
        //then
        Assertions.assertEquals(1, clusterList.size());
    }

    @Test
    @DisplayName("포트와 호스트명으로 클러스터를 찾는다. 그러나 해당 내용이 없었다.")
    public void find_by_host_and_port_cluster_empty(){
        // given
        given(
                clusterRepository.findByHostAndPort(
                        Mockito.anyString(),
                        MockitoHamcrest.intThat(Matchers.greaterThanOrEqualTo(1)
                        )
                )
        ).willReturn(repositoryHelper.getAllClutersForEmpty());

        String host = "host";
        int port = 1;

        //when
        Cluster cluster =  this.clusterService.findByHostAndPort(host, port);

        //then
        Assertions.assertNull(cluster);
    }

    @Test
    @DisplayName("포트와 호스트명으로 클러스터를 찾는다. 그러나 한개만 찾았다.")
    public void find_by_host_and_port_cluster_just_one(){
        // given
        given(
                clusterRepository.findByHostAndPort(
                        Mockito.anyString(),
                        MockitoHamcrest.intThat(Matchers.greaterThanOrEqualTo(1)
                        )
                )
        ).willReturn(repositoryHelper.getAllClutersJustOne());

        String host = "host";
        int port = 1;

        //when
        Cluster cluster =  this.clusterService.findByHostAndPort(host, port);

        //then
        Assertions.assertNotNull(cluster);
    }

    @Test
    @DisplayName("포트와 호스트명으로 클러스터를 찾는다. 그러나 비었다.")
    public void find_by_uuid_cluster_empty(){
        // given
        given(
                clusterRepository.findById(Mockito.any(UUID.class)))
                .willReturn(Optional.empty());
        UUID uuid = UUID.randomUUID();

        //when
        Cluster cluster = this.clusterService.findById(uuid);

        //then
        Assertions.assertNull(cluster);
    }

    @Test
    @DisplayName("포트와 호스트명으로 클러스터를 찾는다. 1개 찾았다.")
    public void find_by_uuid_cluster_just_one(){
        // given
        given(
                        clusterRepository.findById(Mockito.any(UUID.class)))
                .willReturn(Optional.of(repositoryHelper.getCluster()));
        UUID uuid = UUID.randomUUID();

        //when
        Cluster cluster = this.clusterService.findById(uuid);

        //then
        Assertions.assertNotNull(cluster);
    }

    @Test
    @DisplayName("클러스터 추가 성공")
    public void add_cluster_success() throws NullPointerException{
        // given
        UUID uuid = UUID.randomUUID();
        Cluster cluster = new Cluster();
        cluster.setId(uuid);
        given(clusterRepository.save(cluster)).willReturn(cluster);

        //when
        Cluster addedCluster = this.clusterService.add(cluster);

        //then
        Assertions.assertEquals(addedCluster.getId(), cluster.getId());
    }

    @Test
    @DisplayName("클러스터 추가 실패, cluster 객체가 Null")
    public void add_cluster_fail(){
        //when and then
        Assertions.assertThrows(NullPointerException.class, () -> {
            Cluster addedCluster = this.clusterService.add(null);
        });
    }

    @Test
    @DisplayName("클러스터 삭제 성공")
    public void remove_cluster_success(){
        // given
        UUID uuid = UUID.randomUUID();
        Cluster cluster = new Cluster();
        cluster.setId(uuid);
        given(clusterRepository.findById(uuid)).willReturn(Optional.of(cluster));

        //when
        Cluster removedCluster = this.clusterService.remove(uuid);

        //then
        Assertions.assertEquals(removedCluster.getId(), uuid);
    }

    @Test
    @DisplayName("클러스터 삭제 실패, uuid가 없는 아이디 일 경우")
    public void remove_cluster_fail(){
        // given
        UUID uuid = UUID.randomUUID();
        given(clusterRepository.findById(uuid)).willReturn(Optional.empty());

        // then
        Assertions.assertThrows(NoSuchElementException.class, () -> {
            //when
            Cluster removedCluster = this.clusterService.remove(uuid);
        });
    }

    @Test
    @DisplayName("클러스터 삭제 실패, uuid가 없는 아이디 일 경우")
    public void edit_cluster_success() throws NotFoundException {
        // given
        UUID uuid = UUID.randomUUID();
        String name = "hello world!";
        String newName = "hello world!!!!";
        Cluster cluster = new Cluster();
        cluster.setId(uuid);
        cluster.setName(name);

        given(clusterRepository.findById(uuid)).willReturn(Optional.of(cluster));
        cluster.setName(newName);
        given(clusterRepository.save(cluster)).willReturn(cluster);

        //when
        Cluster edittedCluster = this.clusterService.edit(uuid, cluster);
        Assertions.assertEquals(newName, edittedCluster.getName());
    }

    @Test
    @DisplayName("클러스터 상태 스캔 성공")
    public void scan_cluster_state_success(){
        // given
        ClusterStatusRequest clusterStatusRequest = new ClusterStatusRequest();
        clusterStatusRequest.setHost("test");
        clusterStatusRequest.setPort(9200);
        clusterStatusRequest.setScheme("http");

        //when
        // 내부에서 client를 연결하기 때문에 해당 메서드만 fake형태로 사용
        ClusterStatusResponse result = this.fakeClusterService.scanClusterStatus(clusterStatusRequest);

        // then
        Assertions.assertTrue(result.isConnection());
    }

    @Test
    @DisplayName("클러스터 상태 스캔 실패")
    public void scan_cluster_state_fail(){
        // given
        ClusterStatusRequest clusterStatusRequest = new ClusterStatusRequest();
        clusterStatusRequest.setHost("test22");
        clusterStatusRequest.setPort(9200);
        clusterStatusRequest.setScheme("http");

        //when
        // 내부에서 client를 연결하기 때문에 해당 메서드만 fake형태로 사용
        ClusterStatusResponse result = this.fakeClusterService.scanClusterStatus(clusterStatusRequest);

        // then
        Assertions.assertFalse(result.isConnection());
    }

    @Test
    @DisplayName("Save Url 메서드는 현재 사용하지 않는 방식")
    public void test_for_save_url(){

    }
}
