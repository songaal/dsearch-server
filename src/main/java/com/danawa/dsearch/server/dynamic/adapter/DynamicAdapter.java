package com.danawa.dsearch.server.dynamic.adapter;

import com.danawa.dsearch.server.dynamic.entity.DynamicInfo;

import java.util.List;

public interface DynamicAdapter {
    List<DynamicInfo> findAll();

    DynamicInfo findById(Long aLong);

    List<DynamicInfo> saveAll(List<DynamicInfo> readerList);

    void deleteAll(List<DynamicInfo> dynamicInfoList);
}
