package com.netease.ds.fscp.reader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wuqi3@corp.netease.com
 * @created 2020-07-02 15:26
 * @since
 */
public class LocalFileReader extends Reader {


    public static class Job extends Reader.Job {
        private Configuration originConfig;
        private List<FileEntity> fileEntities;

        @Override
        public List<Configuration> split(int adviceNumber) {
            System.out.println("reader split");
            List<Configuration> configurations = new ArrayList<Configuration>();
            int maxTask = this.originConfig.getInt("max-task-count", 1);
            if (maxTask >= this.fileEntities.size()) {
                for (int i = 0; i < this.fileEntities.size(); i++) {
                    Configuration configuration = this.originConfig.clone();
                    List<FileEntity> items = new ArrayList<FileEntity>();
                    items.add(this.fileEntities.get(i));
                    configuration.set("fileEntries", items);
                    configurations.add(configuration);
                }
            } else {
                int count1 = this.fileEntities.size() / maxTask;
                int count2 = this.fileEntities.size() % maxTask;
                int maxCount = count2 == 0 ? count1 : count1 + 1;

                int num = 0;
                List<FileEntity> items = new ArrayList<FileEntity>(maxCount);
                Configuration configuration = this.originConfig.clone();
                for (FileEntity fileEntity : this.fileEntities) {
                    if (num < maxCount) {
                        items.add(fileEntity);
                    } else {
                        configuration.set("fileEntries", items);
                        configurations.add(configuration);
                        configuration.clone();
                        num = 0;
                        configuration = this.originConfig.clone();
                        items = new ArrayList<FileEntity>(maxCount);
                        items.add(fileEntity);
                    }
                    num++;
                }
                configuration.set("fileEntries", items);
                configurations.add(configuration);
            }
            return configurations;
        }

        @Override
        public void init() {
            System.out.println("reader init");
            this.originConfig = super.getPluginJobConf();
            List<FileEntity> fileList = new ArrayList<FileEntity>();
            List<String> fileDir = this.originConfig.getList("src", String.class);
            if (fileDir.isEmpty()) {
                throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "未指定读取路径或文件");
            }
            for (String s : fileDir) {
                File file = new File(s);
                if (!file.exists()) {
                    throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "文件不存在");
                }
                fileList.addAll(getAllFiles(s, ""));
            }
            this.fileEntities = fileList;
        }

        @Override
        public void destroy() {
            System.out.println("reader destroy");
        }
    }

    public static class Task extends Reader.Task {

        @Override
        public void startRead(RecordSender recordSender) {
            System.out.println("reader task start");
            try {
                List<JSONObject> list = this.getPluginJobConf().getList("fileEntries", JSONObject.class);
                for (JSONObject fileEntity : list) {
                    FileEntity fileEntity1 = JSONObject.parseObject(fileEntity.toJSONString(), FileEntity.class);
                    Record record = null;
                    try {
                        record = recordSender.createRecord();
                        record.setDirPath(fileEntity1.getDirRelativePath());
                        record.setFileName(fileEntity1.getFileName());
                        record.setFileInputStream(FileUtils.openInputStream(new File(fileEntity1.getFullPath())));
                        recordSender.sendToWriter(record);
                    } catch (Exception e) {
                        getTaskPluginCollector().collectDirtyRecord(record, e);
                    }
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(CommonErrorCode.RUNTIME_ERROR, e.getMessage(), e);
            }
        }

        @Override
        public void init() {
            System.out.println("reader task init");
        }

        @Override
        public void destroy() {
            System.out.println("reader task destroy");
        }
    }

    /**
     * 获取所有文件的相对路径
     *
     * @param dirRelativePath
     * @return
     */
    private static List<FileEntity> getAllFiles(String contextFilePath, String dirRelativePath) {
        List<FileEntity> result = new ArrayList<FileEntity>();
        File file = new File(StringUtils.isEmpty(dirRelativePath) ? contextFilePath : contextFilePath + "/" + dirRelativePath);
        if (!file.exists()) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "文件不存在");
        }
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File file1 : files) {
                if (file1.isDirectory()) {
                    result.addAll(getAllFiles(contextFilePath, dirRelativePath + "/" + file1.getName()));
                } else {
                    FileEntity fileEntity = new FileEntity();
                    fileEntity.setFileName(file1.getName());
                    fileEntity.setFullPath(contextFilePath + "/" + dirRelativePath + "/" + file1.getName());
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

}
