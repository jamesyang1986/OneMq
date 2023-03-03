package com.qiezi.onemq;

import model.MessageInner;
import model.SaveResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface MessageStore {

    public SaveResult saveMsg(MessageInner msg);

    public CompletableFuture<SaveResult> asyncSaveMsg(MessageInner msg);


    public List<MessageInner> consumerData(String topic, int partition, long startPos, int maxSize);
}
