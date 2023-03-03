package com.qiezi.onemq.serialize;

import model.MessageInner;

public interface IMsgSerializer {

    public byte[] serialize(MessageInner msg);

    public MessageInner deSerialize(byte[] data);

}
