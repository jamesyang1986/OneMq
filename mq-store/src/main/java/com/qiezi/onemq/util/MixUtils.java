package com.qiezi.onemq.util;

import java.text.NumberFormat;

public class MixUtils {

    public static String genFormattedName(long offset) {
        NumberFormat format = NumberFormat.getInstance();
        format.setMinimumIntegerDigits(20);
        format.setMaximumFractionDigits(0);
        format.setGroupingUsed(false);
        String fileName = format.format(offset);
        return fileName;
    }
}
