package com.qiezi.onemq;

import com.qiezi.onemq.store.MappedFileManager;
import model.MessageInner;
import model.SaveResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class DefaultMessageStore implements MessageStore {

    @Override
    public SaveResult saveMsg(MessageInner msg) {
        String topic = msg.getTopic();
        int partion = msg.getPartition();
        return null;
    }

    @Override
    public CompletableFuture<SaveResult> asyncSaveMsg(MessageInner msg) {
        return null;
    }

    @Override
    public List<MessageInner> consumerData(String topic, int partition, long startPos, int maxSize) {
        MappedFileManager mappedFileManager = MappedFileManager.getInstance();


        return null;
    }
}
