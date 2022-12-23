package com.danawa.dsearch.server.dynamic.dto;

import com.danawa.dsearch.server.dynamic.entity.DynamicInfo;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.io.Serializable;

@Value
@AllArgsConstructor
public class DynamicInfoResponse implements Serializable {
    private Long id;
    private String bundleQueue;
    private String bundleServer;
    private String scheme;
    private String ip;
    private String port;
    private String stateEndPoint;
    private String consumeEndPoint;

    public static DynamicInfoResponse of(DynamicInfo dynamicInfo) {
        return new DynamicInfoResponse(dynamicInfo.getId(), dynamicInfo.getBundleQueue(), dynamicInfo.getBundleServer(), dynamicInfo.getScheme(), dynamicInfo.getIp(), dynamicInfo.getPort(), dynamicInfo.getStateEndPoint(), dynamicInfo.getConsumeEndPoint());
    }
}
