package com.qiezi.onemq;

import java.io.File;

public class StoreConfig {
    private static String storePathRootDir = System.getProperty("user.home") + File.separator + "oneMq";

    public static String DEFAULT_MSG_STORE_DIR = storePathRootDir;

    public static final long SEGMENT_FILE_MAX_SIZE = 1024 * 1024 * 500;
}
