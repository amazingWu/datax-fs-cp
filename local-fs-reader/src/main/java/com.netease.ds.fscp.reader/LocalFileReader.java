package com.netease.ds.fscp.reader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * 限制：只能使用MemoryChannel
 *
 * @author wuqi3@corp.netease.com
 * @created 2020-07-02 15:26
 * @since
 */
public class LocalFileReader extends Reader {

    private static final String FILE_ENTITY = "fileEntity";

    public static class Job extends Reader.Job {
        private Configuration originConfig;
        private List<FileEntity> fileEntities;

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>();
            for (FileEntity fileEntity : this.fileEntities) {
                Configuration configuration = this.originConfig.clone();
                configuration.set(FILE_ENTITY, fileEntity);
                configurations.add(configuration);
            }
            return configurations;
        }

        @Override
        public void init() {
            this.originConfig = super.getPluginJobConf();
            List<FileEntity> fileList = new ArrayList<FileEntity>();
            List<String> fileDir = this.originConfig.getList(Key.PATH, String.class);
            if (fileDir.isEmpty()) {
                throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "未指定读取路径或文件");
            }
            for (String s : fileDir) {
                File file = new File(s);
                if (!file.exists()) {
                    throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "文件不存在");
                }
                fileList.addAll(getAllFiles(s, Lists.<String>newArrayList()));
            }
            this.fileEntities = fileList;
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Reader.Task {

        @Override
        public void startRead(RecordSender recordSender) {
            JSONObject fileEntity = this.getPluginJobConf().get(FILE_ENTITY, JSONObject.class);
            FileEntity fileEntity1 = JSONObject.parseObject(fileEntity.toJSONString(), FileEntity.class);
            Record record = null;
            try {
                File file = new File(fileEntity1.getFullPath());
                record = recordSender.createRecord();
                record.setFileLength(file.length());
                record.setDirPath(fileEntity1.getDirRelativePath());
                record.setFileName(fileEntity1.getFileName());
                FileInputStream fileInputStream = new FileInputStream(file);
                record.setFileMd5(DigestUtils.md5Hex(fileInputStream));
                fileInputStream.close();
                record.setFileInputStream(FileUtils.openInputStream(file));
                recordSender.sendToWriter(record);
            } catch (Exception e) {
                if (record != null) {
                    record.destroy();
                }
                throw DataXException.asDataXException(CommonErrorCode.RUNTIME_ERROR, e.getMessage(), e);
            }
        }

        @Override
        public void init() {
        }

        @Override
        public void destroy() {
        }
    }

    /**
     * 获取所有文件的相对路径
     *
     * @param dirRelativePath
     * @return
     */
    private static List<FileEntity> getAllFiles(String contextFilePath, List<String> dirRelativePath) {
        List<FileEntity> result = new ArrayList<FileEntity>();
        File file = new File(dirRelativePath.isEmpty() ? contextFilePath : contextFilePath + File.separator + Record.RecordHelp.getPathString(dirRelativePath, File.separator));
        if (!file.exists()) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "文件不存在");
        }
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File file1 : files) {
                if (file1.isDirectory()) {
                    result.addAll(getAllFiles(contextFilePath, copyAndAdd(dirRelativePath, file1.getName())));
                } else {
                    FileEntity fileEntity = new FileEntity();
                    fileEntity.setFileName(file1.getName());
                    fileEntity.setFullPath(contextFilePath + File.separator + Record.RecordHelp.getPathString(dirRelativePath, File.separator) + File.separator + file1.getName());
                    fileEntity.setDirRelativePath(dirRelativePath);
                    result.add(fileEntity);
                }
            }
        } else {
            FileEntity fileEntity = new FileEntity();
            fileEntity.setFileName(file.getName());
            fileEntity.setFullPath(contextFilePath);
            fileEntity.setDirRelativePath(dirRelativePath);
            result.add(fileEntity);
        }
        return result;
    }

    private static final List<String> copyAndAdd(List<String> collection, String value) {
        List<String> result = Lists.newArrayList(collection);
        result.add(value);
        return result;
    }

}
