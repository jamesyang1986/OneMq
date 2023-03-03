package com.qiezi.onemq.store;

import com.qiezi.onemq.StoreConfig;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MappedFileManager {
    private static Map<String, List<MappedFile>> topicMappedFileMap = new ConcurrentHashMap<>();

    public static final int DEFAULT_PHYCICAL_START_OFFSET = 0;

    public static MappedFile getTopicLatestFile(String topic, int partition) throws IOException {
        String key = topic + "-" + partition;
        if (!topicMappedFileMap.containsKey(key)) {
            MappedFile createdFile = createMappedFile(topic, partition, DEFAULT_PHYCICAL_START_OFFSET);
            List<MappedFile> files = new ArrayList<>();
            files.add(createdFile);
            topicMappedFileMap.put(key, files);
            return createdFile;
        } else {
            List<MappedFile> mappedFiles = topicMappedFileMap.get(key);
            MappedFile lastFile = mappedFiles.get(mappedFiles.size() - 1);
            if (!lastFile.isFull()) {
                return lastFile;
            }

            long totalCount = 0;
            for (MappedFile file : mappedFiles) {
                totalCount += file.getCurrentMsgCount();
            }

            MappedFile createdFile = createMappedFile(topic, partition, totalCount + 1);
            mappedFiles.add(createdFile);
            return createdFile;
        }
    }

    private static synchronized MappedFile createMappedFile(String topic, int partition, long offset) throws IOException {
        String fileName = genFormattedName(offset);
        String filePath = StoreConfig.DEFAULT_MSG_STORE_DIR + "/" + topic + "/"
                + partition + "/" + fileName;
        try {
            return new MappedFile(filePath, StoreConfig.SEGMENT_FILE_MAX_SIZE);
        } catch (IOException e) {
            throw e;
        }
    }


    private static String genFormattedName(long offset) {
        NumberFormat format = NumberFormat.getInstance();
        format.setMinimumIntegerDigits(20);
        format.setMaximumFractionDigits(0);
        format.setGroupingUsed(false);
        String fileName = format.format(offset);
        return fileName;
    }
}
