package com.danawa.dsearch.indexer;

import java.util.Map;

public interface Filter {
    Map<String, Object> filter(Map<String, Object> item);
}
