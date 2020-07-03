package com.alibaba.datax.common.element;

import java.io.InputStream;
import java.util.List;

/**
 * @author wuqi3
 * @date 20-07-07
 */
public interface Record {

    /**
     * 源文件的流
     *
     * @return
     */
    InputStream getInputStream();

    /**
     * 获取文件md5
     *
     * @return
     */
    String getFileMd5();

    /**
     * 设置文件md5
     *
     * @param md5
     */
    void setFileMd5(String md5);

    /**
     * 设置文件byte长度
     *
     * @param fileLength
     */
    void setFileLength(Long fileLength);

    /**
     * 获取文件byte长度
     *
     * @return
     */
    Long getFileLength();

    /**
     * 获取文件所在目录的相对路径层次
     *
     * @return
     */
    List<String> getDirPath();

    /**
     * 获取文件名
     *
     * @return
     */
    String getFileName();

    /**
     * 设置文件所在目录的相对路径层次
     *
     * @param filePath 按层次深度排序
     */
    void setDirPath(List<String> filePath);

    /**
     * 设置文件流
     *
     * @param inputStream
     */
    void setFileInputStream(InputStream inputStream);

    /**
     * 设置文件名称
     *
     * @param fileName
     */
    void setFileName(String fileName);

    /**
     * 销毁方法，释放资源，用于有中间态的
     */
    void destroy();

    class RecordHelp {

        public static String getPathString(List<String> dirPath, String separator) {
            if (dirPath == null) {
                return "";
            }
            boolean first = true;
            StringBuilder sb = new StringBuilder();
            for (String item : dirPath) {
                if (!first) {
                    sb.append(separator);
                }
                sb.append(item);
                first = false;
            }
            return sb.toString();
        }
    }
}
