package com.alibaba.datax.core.statistics.plugin.task.util;

import com.alibaba.datax.common.element.Record;

import java.io.File;
import java.io.InputStream;
import java.util.List;

public class DirtyRecord implements Record {
    private List<String> dirPath;
    private String fileName;
    private Long fileLength;
    private String fileMd5;

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
    public String getFileMd5() {
        return fileMd5;
    }

    @Override
    public void setFileMd5(String md5) {
        this.fileMd5 = md5;
    }

    @Override
    public void setFileLength(Long fileLength) {
        this.fileLength = fileLength;
    }

    @Override
    public Long getFileLength() {
        return this.fileLength;
    }

    @Override
    public List<String> getDirPath() {
        return dirPath;
    }

    @Override
    public String getFileName() {
        return this.fileName;
    }

    @Override
    public void setDirPath(List<String> filePath) {
        this.dirPath = filePath;
    }

    @Override
    public void setFileInputStream(InputStream inputStream) {

    }

    @Override
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void destroy() {

    }

    @Override
    public String toString() {
        return "DirtyRecord{" +
                "dirPath=" + Record.RecordHelp.getPathString(dirPath, File.separator) +
                ", fileName='" + fileName + '\'' +
                ", fileLength=" + fileLength +
                ", fileMd5='" + fileMd5 + '\'' +
                '}';
    }
}
