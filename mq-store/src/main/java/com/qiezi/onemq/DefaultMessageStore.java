package com.qiezi.onemq;

import model.MessageInner;
import model.SaveResult;

import java.util.concurrent.CompletableFuture;

public class DefaultMessageStore implements MessageStore {

    @Override
    public SaveResult saveMsg(MessageInner msg) {
        return null;
    }

    @Override
    public CompletableFuture<SaveResult> asyncSaveMsg(MessageInner msg) {
        return null;
    }
}
