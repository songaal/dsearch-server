package com.danawa.dsearch.server.dynamicIndex.dto;

import com.danawa.dsearch.server.dynamicIndex.entity.DynamicIndexInfo;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.io.Serializable;

@Value
@AllArgsConstructor
public class DynamicIndexInfoResponse implements Serializable {
    private Long id;
    private String bundleQueue;
    private String bundleServer;
    private String scheme;
    private String ip;
    private String port;
    private String stateEndPoint;
    private String consumeEndPoint;

    public static DynamicIndexInfoResponse of(DynamicIndexInfo dynamicIndexInfo) {
        return new DynamicIndexInfoResponse(dynamicIndexInfo.getId(), dynamicIndexInfo.getBundleQueue(), dynamicIndexInfo.getBundleServer(), dynamicIndexInfo.getScheme(), dynamicIndexInfo.getIp(), dynamicIndexInfo.getPort(), dynamicIndexInfo.getStateEndPoint(), dynamicIndexInfo.getConsumeEndPoint());
    }
}
