package com.alibaba.datax.core.transport.record;

import com.alibaba.datax.common.element.Record;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.util.List;

/**
 * Created by jingxing on 14-8-24.
 */

public class DefaultRecord implements Record {

    private InputStream inputStream;
    private String fileName;

    private List<String> dirPath;

    private String fileMd5;

    private Long fileLength;

    @Override
    public InputStream getInputStream() {
        return inputStream;
    }

    @Override
    public String getFileMd5() {
        return fileMd5;
    }

    @Override
    public void setFileMd5(String fileMd5){
        this.fileMd5 = fileMd5;
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
    public void setDirPath(List<String> inputPath) {
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

    @Override
    public void destroy() {
        if (inputStream != null) {
            IOUtils.closeQuietly(inputStream);
        }
    }
}
