package com.danawa.dsearch.server.dynamicIndex.adapter;

import com.danawa.dsearch.server.dynamicIndex.entity.DynamicIndexInfo;
import com.danawa.dsearch.server.dynamicIndex.repository.DynamicIndexDatabaseRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.NoSuchElementException;

@Service
public class DynamicIndexDatabaseAdapter implements DynamicIndexAdapter {
    private static Logger logger = LoggerFactory.getLogger(DynamicIndexDatabaseAdapter.class);
    private DynamicIndexDatabaseRepository dynamicIndexDatabaseRepository;

    public DynamicIndexDatabaseAdapter(DynamicIndexDatabaseRepository dynamicIndexDatabaseRepository){
        this.dynamicIndexDatabaseRepository = dynamicIndexDatabaseRepository;
    }

    @Override
    public List<DynamicIndexInfo> findAll() {
        return dynamicIndexDatabaseRepository.findAll();
    }

    @Override
    public DynamicIndexInfo findById(Long id) throws NoSuchElementException {
        try {
            return dynamicIndexDatabaseRepository.findById(id).get();
        } catch (NoSuchElementException e) {
            throw new NoSuchElementException(e.getMessage());
        }
    }

    @Override
    public List<DynamicIndexInfo> saveAll(List<DynamicIndexInfo> readerList) {
        return dynamicIndexDatabaseRepository.saveAll(readerList);
    }

    @Override
    public void deleteAll(List<DynamicIndexInfo> dynamicIndexInfoList) {
        dynamicIndexDatabaseRepository.deleteAll(dynamicIndexInfoList);
    }

    @Override
    public void flush() {
        dynamicIndexDatabaseRepository.flush();
    }


}
