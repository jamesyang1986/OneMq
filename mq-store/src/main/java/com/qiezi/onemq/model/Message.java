package com.qiezi.onemq.model;

import java.util.Map;

public class Message {
    private String topic;

    private long bornTs;

    private long saveTs;

    private byte[] contents;

    private Map<String, String> properties;


}
