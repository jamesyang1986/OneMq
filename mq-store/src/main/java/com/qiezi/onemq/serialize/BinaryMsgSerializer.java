package com.qiezi.onemq.serialize;

import model.MessageInner;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class BinaryMsgSerializer implements IMsgSerializer {

    public static final byte MAGIC_FIRST_BYTE = (byte) 0xAA;

    public static final byte MAGIC_SECOND_BYTE = (byte) 0xAA;

    @Override
    public byte[] serialize(MessageInner msg) {
        ByteBuffer header = ByteBuffer.allocate(4 + 4);
        header.put(MAGIC_FIRST_BYTE);
        header.put(MAGIC_SECOND_BYTE);

        //msgType
        header.put((byte) 1);

        //compressType
        header.put((byte) 0);

        //serialize type


        try {
            byte[] data = msg.getContent();
            int len = data.length;
            header.putInt(len);
            ByteBuffer toSaveBuffer = ByteBuffer.allocate(8 + len);
            toSaveBuffer.put(header.array());
            toSaveBuffer.put(data);
            return toSaveBuffer.array();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public MessageInner deSerialize(byte[] data) {
        ByteBuffer dataBuffer = ByteBuffer.wrap(data);
        byte firstByte = dataBuffer.get();
        byte secondByte = dataBuffer.get();

        if ((firstByte != (byte) 0xAA) || (secondByte != (byte) 0xBB)) {
            throw new IllegalArgumentException("wrong msg magic num!!!");
        }

        byte msgType = dataBuffer.get();
        byte compressType = dataBuffer.get();

        int len = dataBuffer.getInt();
        System.out.println("the len is:" + len);

        byte[] content = new byte[len];
        dataBuffer.get(content);

        MessageInner msg = new MessageInner();
        msg.setContent(content);
        return msg;
    }
}
