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

    private volatile int commitOffset;

    // the max segment file size set to 500Mb
    private long fileSize;
    private long msgCount;
    private boolean isFull = false;

    private static Logger LOG = LoggerFactory.getLogger(MappedFile.class);

    public static final int OS_PAGE_SIZE = 4 * 1024;

    public MappedFile(String topic, int partition, long startOffset) throws IOException {
        this.topic = topic;
        this.partition = partition;
        this.startOffset = startOffset;
        String fileName = StoreConfig.DEFAULT_MSG_STORE_DIR + "/" + this.topic + "/"
                + this.partition + "/" + this.startOffset;
        createMappedFile(fileName, StoreConfig.SEGMENT_FILE_MAX_SIZE);
    }

    public MappedFile(String fileName, long fileSize) throws IOException {
        createMappedFile(fileName, fileSize);
    }

    private void createMappedFile(String fileName, long fileSize) throws IOException {
        this.fileSize = fileSize;
        this.file = new File(fileName);
        ensureDirOk(this.file);
        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.fileSize);

            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    LOG.info("start bg thread to run the disk data syncer....");
                    sync2Disk();
                }
            });
            thread.setName("disk-syncer-" + this.file.getName());
            thread.start();


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

    public synchronized boolean saveData(ByteBuffer byteBuffer) {
        byteBuffer.flip();
        int size = byteBuffer.remaining();
        if (size + this.writeOffset > this.fileSize) {
            this.isFull = true;
            return false;
        }

        while (byteBuffer.hasRemaining()) {
            ByteBuffer fileBuffer = this.mappedByteBuffer.slice();
            fileBuffer.position(this.writeOffset);
            fileBuffer.put(byteBuffer);
        }

        this.writeOffset += size;
        this.msgCount++;

//        sync2Disk();

        return true;
    }


    private void sync2Disk() {
        //force to flush to disk, only for test.
        while (true) {
            if (this.writeOffset - this.commitOffset > 100 * OS_PAGE_SIZE) {
                this.mappedByteBuffer.force();
                this.commitOffset = this.writeOffset;
            }

            if (this.writeOffset == this.commitOffset && isFull) {
                return;
            }

            try {
                // the thread sleep 10ms
                Thread.sleep(10);
            } catch (InterruptedException e) {
            }

        }
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

    public boolean isFull() {
        try {
            return isFull;
//            return this.fileChannel.position() >= this.fileSize;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public long getCurrentPhysicalOffset() {
        return this.writeOffset;
    }

    public long getCurrentMsgCount() {
        return this.msgCount;
    }

    public String getFileName() {
        return this.file.getName();
    }

}
