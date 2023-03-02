package com.qiezi.onemq.store;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MappedFileManager {
    private static Map<String, List<MappedFile>> topicMappedFileMap = new ConcurrentHashMap<>();

    public MappedFile getTopicLatestFile(String topic, int partion) {
        String key = topic + "-" + partion;
        if (!topicMappedFileMap.containsKey(key)) {


        }

        return null;
    }
}
