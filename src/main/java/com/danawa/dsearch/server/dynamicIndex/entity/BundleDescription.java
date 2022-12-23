package com.danawa.dsearch.server.dynamicIndex.entity;

public enum BundleDescription {
    SERVER("server"),
    QUEUE("queue"),
    UNKNOWN("unknown");

    private String name;

    BundleDescription(String name){
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static BundleDescription getDescription(String desc){
        if(desc.toLowerCase().equals("server")){
            return BundleDescription.SERVER;
        }else if(desc.toLowerCase().equals("queue")){
            return BundleDescription.QUEUE;
        }else{
            return BundleDescription.UNKNOWN;
        }
    }
}
