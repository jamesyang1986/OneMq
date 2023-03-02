package com.qiezi.onemq;

import model.MessageInner;
import model.SaveResult;

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
}
