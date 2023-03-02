package com.qiezi.onemq.store;

import com.qiezi.onemq.StoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedFile {
    private String topic;
    private int partition;
    private long startOffset;
    private File file;
    private FileChannel fileChannel;
    protected MappedByteBuffer mappedByteBuffer;

    private volatile int writeOffset;

    // the max segment file size set to 500Mb

    private static Logger LOG = LoggerFactory.getLogger(MappedFile.class);

    public MappedFile(String topic, int partition, long startOffset) throws IOException {
        this.topic = topic;
        this.partition = partition;
        this.startOffset = startOffset;
        String fileName = StoreConfig.DEFAULT_MSG_STORE_DIR + "/" + this.topic + "/"
                + this.partition + "/" + this.startOffset;
        this.file = new File(fileName);
        ensureDirOk(this.file);

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, StoreConfig.SEGMENT_FILE_MAX_SIZE);
        } catch (Exception e) {
            LOG.error("fail to create mapped file, topic:{}, partition:{},startOffset:{}",
                    topic, partition, startOffset, e);
            throw e;
        }

    }

    private void ensureDirOk(File file) {
        if (file != null && !file.exists()) {
            String parentDirPath = this.file.getParent();
            File parentDir = new File(parentDirPath);
            if (!parentDir.exists()) {
                LOG.info("create dir:{}", parentDir);
                parentDir.mkdirs();
            }
        }
    }

    public synchronized void saveData(ByteBuffer byteBuffer) {
        byteBuffer.flip();
        int size = byteBuffer.remaining();

        while (byteBuffer.hasRemaining()) {
            ByteBuffer fileBuffer = this.mappedByteBuffer.slice();
            fileBuffer.position(this.writeOffset);
            fileBuffer.put(byteBuffer);
            //force to flush to disk, only for test.
        }
        this.mappedByteBuffer.force();
        this.writeOffset += size;
    }

    public ByteBuffer readData(long pos, int readSize) {
        if (writeOffset != 0 && (pos + readSize > writeOffset)) {
            throw new RuntimeException("read out of index for file length.");
        }

        ByteBuffer returnData = ByteBuffer.allocate(readSize);
        try {
            int readNum = this.fileChannel.read(returnData, pos);
            if (readNum != readSize) {
                throw new RuntimeException("read file error : expect: " + readSize + " actually:" + readNum);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return returnData;
    }


}
