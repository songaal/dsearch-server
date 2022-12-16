package com.danawa.dsearch.server.dynamic.adapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DynamicDbAdapter implements DynamicAdapter {
    private static Logger logger = LoggerFactory.getLogger(DynamicDbAdapter.class);
    //private DynamicDbRepository dynamicDbRepository;

    //public DynamicDbAdapter(DynamicDbRepository dynamicDbRepository){
    //    this.dynamicDbRepository = dynamicDbRepository;
    //}
/*
    @Override
    public List<DynamicInfo> findAll() {
        return dynamicDbRepository.findAll();
    }

    @Override
    public DynamicInfo findById(Long id) {
        try {
            return dynamicDbRepository.findById(id).get();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public List<DynamicInfo> saveAll(List<DynamicInfo> readerList) {
        return dynamicDbRepository.saveAll(readerList);
    }

    @Override
    public void deleteAll(List<DynamicInfo> dynamicInfoList) {
        dynamicDbRepository.deleteAll(dynamicInfoList);
    }*/
}
