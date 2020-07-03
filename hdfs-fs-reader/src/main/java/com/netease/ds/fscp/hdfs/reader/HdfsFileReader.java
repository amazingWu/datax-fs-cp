package com.netease.ds.fscp.hdfs.reader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wuqi3@corp.netease.com
 * @created 2020-07-06 17:53
 * @since
 */
public class HdfsFileReader extends Reader {
    private static final String FILE_ENTITY = "fileEntity";

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration readerOriginConfig = null;
        private DFSUtil dfsUtil = null;
        private List<String> path = null;
        private String defaultFS;
        private List<FileEntity> fileEntities;

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>();
            for (FileEntity fileEntity : this.fileEntities) {
                Configuration configuration = this.readerOriginConfig.clone();
                configuration.set(FILE_ENTITY, fileEntity);
                configurations.add(configuration);
            }
            return configurations;
        }

        @Override
        public void init() {
            LOG.info("init() begin...");
            this.readerOriginConfig = super.getPluginJobConf();
            this.validate();
            this.dfsUtil = new DFSUtil();
            this.defaultFS = this.readerOriginConfig.getString(Key.DEFAULT_FS);
            dfsUtil.getFileSystem(this.defaultFS, readerOriginConfig);
            this.fileEntities = Lists.newLinkedList();
            for (String pathItem : this.path) {
                fileEntities.addAll(dfsUtil.getHDFSAllFiles(pathItem));
            }
            LOG.info("init() ok and end...");
        }

        @Override
        public void destroy() {
            // FileSystem中带有缓存，如果reader和writer同时使用了HDFS，在此处如果关闭了FileSystem可能会导致Writer的失败
        }

        private void validate() {
            String pathInString = this.readerOriginConfig.getNecessaryValue(Key.PATH, HdfsReaderErrorCode.REQUIRED_VALUE);
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<String>();
                path.add(pathInString);
            } else {
                path = this.readerOriginConfig.getList(Key.PATH, String.class);
                if (null == path || path.size() == 0) {
                    throw DataXException.asDataXException(HdfsReaderErrorCode.REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
                }
                for (String eachPath : path) {
                    if (!eachPath.startsWith("/")) {
                        String message = String.format("请检查参数path:[%s],需要配置为绝对路径", eachPath);
                        throw DataXException.asDataXException(HdfsReaderErrorCode.ILLEGAL_VALUE, message);
                    }
                }
            }
            //check Kerberos
            Boolean haveKerberos = this.readerOriginConfig.getBool(Key.HAVE_KERBEROS, false);
            if (haveKerberos) {
                this.readerOriginConfig.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, HdfsReaderErrorCode.REQUIRED_VALUE);
                this.readerOriginConfig.getNecessaryValue(Key.KERBEROS_PRINCIPAL, HdfsReaderErrorCode.REQUIRED_VALUE);
            }
        }
    }

    public static class Task extends Reader.Task {
        private DFSUtil hdfsHelper = null;
        private Configuration readerSliceConfig;

        private String defaultFS;

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
                record.setFileLength(fileEntity1.getFileLength());
                record.setFileInputStream(hdfsHelper.getInputStream(fileEntity1.getFullPath()));
                recordSender.sendToWriter(record);
            } catch (Exception e) {
                if (record != null) {
                    record.destroy();
                }
                // 放弃对脏数据支持，因为对文件的处理要么支持重试要么支持脏数据，所以选择支持重试而放弃脏数据的支持
                // getTaskPluginCollector().collectDirtyRecord(record, e);
                throw DataXException.asDataXException(CommonErrorCode.RUNTIME_ERROR, e.getMessage(), e);
            }
        }

        @Override
        public void init() {
            this.readerSliceConfig = this.getPluginJobConf();
            this.defaultFS = this.readerSliceConfig.getString(Key.DEFAULT_FS);
            hdfsHelper = new DFSUtil();
            hdfsHelper.getFileSystem(defaultFS, readerSliceConfig);
        }

        @Override
        public void destroy() {
        }
    }
}
