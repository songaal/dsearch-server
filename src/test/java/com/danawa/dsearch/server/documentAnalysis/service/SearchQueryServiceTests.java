package com.danawa.dsearch.server.documentAnalysis.service;

import com.danawa.dsearch.server.documentAnalysis.adapter.SearchQueryAdapter;
import com.danawa.dsearch.server.documentAnalysis.dto.SearchQueryCreateRequest;
import com.danawa.dsearch.server.documentAnalysis.dto.SearchQueryUpdateRequest;
import com.danawa.dsearch.server.documentAnalysis.entity.SearchQuery;
import com.danawa.dsearch.server.documentAnalysis.repository.SearchQueryRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SearchQueryServiceTests {

    private SearchQueryService searchQueryService;

    @Mock
    private SearchQueryAdapter searchQueryAdapter;

    @BeforeEach
    public void setup(){
        this.searchQueryService = new SearchQueryService(searchQueryAdapter);
    }

    @Test
    @DisplayName("기동 시 searchQuery 저장 인덱스 생성 성공")
    public void initialize_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        doNothing().when(searchQueryAdapter).initialize(clusterId);

        Assertions.assertDoesNotThrow(() -> {
            searchQueryService.initialize(clusterId);
        });

        verify(searchQueryAdapter).initialize(clusterId);
    }

    @Test
    @DisplayName("기동 시 searchQuery 저장 인덱스 생성 실패")
    public void initialize_fail() throws IOException {
        UUID clusterId = UUID.randomUUID();
        doThrow(IOException.class).when(searchQueryAdapter).initialize(clusterId);

        Assertions.assertThrows(IOException.class, () -> {
            searchQueryService.initialize(clusterId);
        });
    }

    @Test
    @DisplayName("searchQuery 저장 성공")
    public void create_searchQuery_success() {
        UUID clusterId = UUID.randomUUID();
        String id = "1";
        String index = "test";
        String name = "test";
        String query = "{ \"query\": { \"match_all\": {}}}";

        SearchQueryCreateRequest searchQueryCreateRequest = new SearchQueryCreateRequest();
        searchQueryCreateRequest.setIndex(index);
        searchQueryCreateRequest.setName(name);
        searchQueryCreateRequest.setQuery(query);

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setId(id);
        searchQuery.setQuery(query);
        searchQuery.setIndex(index);
        searchQuery.setName(name);

        when(searchQueryAdapter.create(eq(clusterId), any(SearchQuery.class))).thenReturn(searchQuery);
        SearchQuery result = searchQueryService.createSearchQuery(clusterId, searchQueryCreateRequest);
        Assertions.assertEquals(id, result.getId());
        Assertions.assertEquals(index, result.getIndex());
        Assertions.assertEquals(query, result.getQuery());
        Assertions.assertEquals(name, result.getName());
    }

    @Test
    @DisplayName("searchQuery 저장 실패")
    public void create_searchQuery_fail() {
        UUID clusterId = UUID.randomUUID();
        String id = "1";
        String index = "test";
        String name = "test";
        String query = "{ \"query\": { \"match_all\": {}}}";

        SearchQueryCreateRequest searchQueryCreateRequest = new SearchQueryCreateRequest();
        searchQueryCreateRequest.setIndex(index);
        searchQueryCreateRequest.setName(name);
        searchQueryCreateRequest.setQuery(query);

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setId(id);
        searchQuery.setQuery(query);
        searchQuery.setIndex(index);
        searchQuery.setName(name);

        when(searchQueryAdapter.create(eq(clusterId), any(SearchQuery.class))).thenReturn(null);
        SearchQuery result = searchQueryService.createSearchQuery(clusterId, searchQueryCreateRequest);
        Assertions.assertNull(result);
    }

    @Test
    @DisplayName("searchQuery 삭제 성공")
    public void delete_searchQuery_success() {
        UUID clusterId = UUID.randomUUID();
        String id = "1";

        doNothing().when(searchQueryAdapter).deleteById(clusterId, id);

        searchQueryService.deleteSearchQuery(clusterId, id);

        verify(searchQueryAdapter, times(1)).deleteById(clusterId, id);
    }

    @Test
    @DisplayName("searchQuery 업데이트 성공")
    public void update_searchQuery_success() {
        UUID clusterId = UUID.randomUUID();
        String id = "1";
        String index = "test";
        String name = "test";
        String query = "{ \"query\": { \"match_all\": {}}}";
        String queryNew = "{ \"query\": { \"match\": { \"productName\": \"a\" }}}";

        SearchQueryUpdateRequest searchQueryUpdateRequest = new SearchQueryUpdateRequest();
        searchQueryUpdateRequest.setId(id);
        searchQueryUpdateRequest.setIndex(index);
        searchQueryUpdateRequest.setName(name);
        searchQueryUpdateRequest.setQuery(queryNew);

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setId(id);
        searchQuery.setQuery(query);
        searchQuery.setIndex(index);
        searchQuery.setName(name);

        SearchQuery searchNewQuery = new SearchQuery();
        searchNewQuery.setId(id);
        searchNewQuery.setQuery(queryNew);
        searchNewQuery.setIndex(index);
        searchNewQuery.setName(name);

        List<SearchQuery> searchQueryList = new ArrayList();
        searchQueryList.add(searchQuery);

        when(searchQueryAdapter.findAll(eq(clusterId))).thenReturn(searchQueryList);
        List<SearchQuery> list = searchQueryService.getSearchQueryList(clusterId);

        when(searchQueryAdapter.update(eq(clusterId), any(SearchQuery.class))).thenReturn(searchNewQuery);
        SearchQuery result = searchQueryService.updateSearchQuery(clusterId, searchQueryUpdateRequest);


        Assertions.assertEquals(list.get(0).getId(), result.getId());
        Assertions.assertEquals(list.get(0).getIndex(), result.getIndex());
        Assertions.assertNotEquals(list.get(0).getQuery(), result.getQuery());
        Assertions.assertEquals(list.get(0).getName(), result.getName());
    }
}
