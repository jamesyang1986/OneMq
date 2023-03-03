package com.qiezi.onemq.store;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class MappedFileTest {
    public static final String testStr = "asdfdasfadsfqwreqwrq3asdfadsfasdf";

    @org.junit.Before
    public void setUp() throws Exception {
    }

    @org.junit.After
    public void tearDown() throws Exception {
    }

    @org.junit.Test
    public void saveData() {
        System.out.println("test msg size:" + testStr.getBytes().length);
        MappedFile file = null;
        for (int i = 0; i < 1000000000; i++) {
            try {
                file = MappedFileManager.getTopicLatestFile("test2-topic", 0);
                boolean result = writeMsg(file, testStr + "--------" + i);
                if (!result && file.isFull()) {
                    file = MappedFileManager.getTopicLatestFile("test2-topic", 0);
                    System.out.println("use new file to create :" + file.getFileName());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
        String msg = readOneMsg(file, 0);
        assertEquals(testStr, msg);
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


    private boolean writeMsg(MappedFile file, String contents) {
        ByteBuffer header = ByteBuffer.allocate(4 + 4);
        header.put((byte) 0xAA);
        header.put((byte) 0xBB);
        //msgType
        header.put((byte) 1);
        //compressType
        header.put((byte) 0);

        try {
            byte[] data = contents.getBytes("utf-8");
            int len = data.length;

            header.putInt(len);

            ByteBuffer toSaveBuffer = ByteBuffer.allocate(8 + len);
            toSaveBuffer.put(header.array());
            toSaveBuffer.put(data);

            return file.saveData(toSaveBuffer);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @org.junit.Test
    public void readOneMsg() {
        MappedFile file = null;
        try {
            file = new MappedFile("test-topic", 0, 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String msg = readOneMsg(file, 0);
        assertEquals(testStr, msg);

    }
}