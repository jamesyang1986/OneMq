package com.qiezi.onemq;

import model.MessageInner;
import model.SaveResult;

import java.util.concurrent.CompletableFuture;

public interface MessageStore {

    public SaveResult saveMsg(MessageInner msg);

    public CompletableFuture<SaveResult> asyncSaveMsg(MessageInner msg);
}
