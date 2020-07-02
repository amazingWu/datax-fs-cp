package com.alibaba.datax.core.statistics.plugin.task.util;

import com.alibaba.datax.common.element.Record;

import java.io.InputStream;

public class DirtyRecord implements Record {
    private String dirPath;
    private String fileName;

    public static DirtyRecord asDirtyRecord(final Record record) {
        DirtyRecord dirtyRecord = new DirtyRecord();
        dirtyRecord.dirPath = record.getDirPath();
        dirtyRecord.fileName = record.getFileName();
        return dirtyRecord;
    }

    @Override
    public InputStream getInputStream() {
        return null;
    }

    @Override
    public String getDirPath() {
        return dirPath;
    }

    @Override
    public String getFileName() {
        return this.fileName;
    }

    @Override
    public void setDirPath(String filePath) {
        this.dirPath = filePath;
    }

    @Override
    public void setFileInputStream(InputStream inputStream) {

    }

    @Override
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
