package com.danawa.dsearch.server.dynamicIndex.adapter;

import com.danawa.dsearch.server.dynamicIndex.entity.DynamicIndexInfo;

import java.util.List;
import java.util.NoSuchElementException;

public interface DynamicIndexAdapter {
    List<DynamicIndexInfo> findAll();

    DynamicIndexInfo findById(Long aLong) throws NoSuchElementException;

    List<DynamicIndexInfo> saveAll(List<DynamicIndexInfo> readerList);

    void deleteAll(List<DynamicIndexInfo> dynamicIndexInfoList);

    void flush();
}
