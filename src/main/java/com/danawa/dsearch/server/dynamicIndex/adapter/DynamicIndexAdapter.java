package com.danawa.dsearch.server.dynamicIndex.adapter;

import com.danawa.dsearch.server.dynamicIndex.entity.DynamicIndexInfo;

import java.util.List;

public interface DynamicIndexAdapter {
    List<DynamicIndexInfo> findAll();

    DynamicIndexInfo findById(Long aLong);

    List<DynamicIndexInfo> saveAll(List<DynamicIndexInfo> readerList);

    void deleteAll(List<DynamicIndexInfo> dynamicIndexInfoList);

    void flush();
}
