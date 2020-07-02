package com.alibaba.datax.common.element;

import java.io.InputStream;

/**
 * Created by jingxing on 14-8-24.
 */

public interface Record {

    /**
     * 源文件的流
     *
     * @return
     */
    InputStream getInputStream();

    /**
     * 所在目录的相对路径
     *
     * @return
     */
    String getDirPath();

    String getFileName();

    void setDirPath(String filePath);

    void setFileInputStream(InputStream inputStream);

    void setFileName(String fileName);
}
