package com.alibaba.datax.core.transport.record;

import com.alibaba.datax.common.element.Record;

import java.io.InputStream;

/**
 * Created by jingxing on 14-8-24.
 */

public class DefaultRecord implements Record {

    private InputStream inputStream;
    private String fileName;

    private String dirPath;

    @Override
    public InputStream getInputStream() {
        return inputStream;
    }

    @Override
    public String getDirPath() {
        return dirPath;
    }


    @Override
    public void setDirPath(String inputPath) {
        this.dirPath = inputPath;
    }


    @Override
    public void setFileInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
