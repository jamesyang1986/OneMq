package com.qiezi.onemq.store;

import com.qiezi.onemq.StoreConfig;
import com.qiezi.onemq.util.MixUtils;
import model.MessageInner;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MappedFileManager {
    private static Map<String, List<MappedFile>> topicMappedFileMap = new ConcurrentHashMap<>();
    public static final int DEFAULT_PHYCICAL_START_OFFSET = 0;
    private String storeDir;

    private MappedFileManager() {
        this(StoreConfig.DEFAULT_MSG_STORE_DIR);
    }

    private MappedFileManager(String storeDir) {
        this.storeDir = storeDir;
        load();
    }

    private static MappedFileManager mappedFileManager;

    public static MappedFileManager getInstance() {
        synchronized (MappedFileManager.class) {
            if (mappedFileManager == null) {
                synchronized (MappedFileManager.class) {
                    if (mappedFileManager == null) {
                        mappedFileManager = new MappedFileManager();
                    }
                }
            }
        }
        return mappedFileManager;
    }

    public static MappedFile getTopicLatestFile(String topic, int partition) throws IOException {
        String key = genKey(topic, partition);
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


    private static String genKey(String topic, int partition) {
        String key = topic + "-" + partition;
        return key;
    }

    private static synchronized MappedFile createMappedFile(String topic, int partition, long offset) throws IOException {
        String fileName = MixUtils.genFormattedName(offset);
        String filePath = StoreConfig.DEFAULT_MSG_STORE_DIR + "/" + topic + "/"
                + partition + "/" + fileName;
        try {
            return new MappedFile(filePath, StoreConfig.SEGMENT_FILE_MAX_SIZE);
        } catch (IOException e) {
            throw e;
        }
    }


    public void load() {
        File mqDir = new File(storeDir);
        if (mqDir == null || !mqDir.isDirectory()) {
            throw new IllegalStateException("load msg files error.");
        }

        for (File topicDir : mqDir.listFiles()) {
            String topic = topicDir.getName();
            for (File partitionDir : topicDir.listFiles()) {
                String partition = partitionDir.getName();
                String key = topic + "-" + partition;
                File[] dataFiles = partitionDir.listFiles();

                for (File dataFile : dataFiles) {
                    String fileOffset = dataFile.getName();
                    try {
                        MappedFile createdFile =
                                new MappedFile(dataFile.getAbsolutePath(), DEFAULT_PHYCICAL_START_OFFSET);
                        topicMappedFileMap.putIfAbsent(key, new ArrayList<>()).add(createdFile);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }
            }

        }
    }


    public List<MessageInner> consumerData(String topic, int partition, long startPos, int maxSize) {
        List<MessageInner> dataList = new ArrayList<>();
        String key = genKey(topic, partition);
        List<MappedFile> mappedFiles = topicMappedFileMap.get(key);

        String fileName = MixUtils.genFormattedName(startPos);

        MappedFile targetFile = findMatchedFile(startPos, mappedFiles);
        while (targetFile != null && maxSize > 0) {


            maxSize--;
        }

        return dataList;

    }


    private MappedFile findMatchedFile(long startPos, List<MappedFile> mappedFiles) {
        MappedFile targetFile = null;
        for (int i = 0; i < mappedFiles.size(); i++) {
            String startFileName = mappedFiles.get(i).getFileName();
            long aPos = Long.parseLong(startFileName);

            if (mappedFiles.size() == 1 && aPos <= startPos) {
                targetFile = mappedFiles.get(i);
                break;
            }

            String nextFileName = mappedFiles.get(i + 1).getFileName();
            long bPos = Long.parseLong(nextFileName);

            if (aPos <= startPos && startPos < bPos) {
                targetFile = mappedFiles.get(i);
                break;
            }
        }

        return targetFile;
    }

    private String readOneMsg(MappedFile file, long pos) {
        ByteBuffer header = file.readData(pos, 8);
        header.flip();
        byte firstByte = header.get();
        byte secondByte = header.get();

        if ((firstByte != (byte) 0xAA) || (secondByte != (byte) 0xBB)) {
            throw new IllegalArgumentException("wrong msg magic num!!!");
        }

        byte msgType = header.get();
        byte compressType = header.get();

        int len = header.getInt();
        System.out.println("the len is:" + len);

        ByteBuffer msgData = file.readData(pos + 8, len);
        try {
            String msg = new String(msgData.array(), "utf-8");
            System.out.println(msg);
            return msg;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
