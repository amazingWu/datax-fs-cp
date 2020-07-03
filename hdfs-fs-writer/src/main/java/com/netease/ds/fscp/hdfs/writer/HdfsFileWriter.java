package com.netease.ds.fscp.hdfs.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * @author wuqi3@corp.netease.com
 * @created 2020-07-03 14:18
 * @since
 */
public class HdfsFileWriter extends Writer {
    private static final String TMP_DIR = "tmpDir";
    /**
     * 如果同名文件存在，则直接覆盖
     */
    private static final String OVER_WRITE = "overwrite";
    /**
     * 如果同名文件存在，如果文件长度一致，则跳过
     */
    private static final String UPDATE = "update";
    /**
     * 如果目标路径存在则删除
     */
    private static final String DELETE_IF_EXIST = "deleteifexist";

    public static class Job extends Writer.Job {
        private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);
        private Configuration configuration;
        private String defaultFS;
        private Path path;
        private HdfsHelper hdfsHelper;
        private Path tmpDirPath;
        private String mode;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(this.configuration.clone());
            }
            return configurations;
        }

        @Override
        public void init() {
            this.configuration = super.getPluginJobConf();
            this.defaultFS = this.configuration.getNecessaryValue(Key.DEFAULT_FS, HdfsWriterErrorCode.REQUIRED_VALUE);
            this.validate();
            hdfsHelper = new HdfsHelper();
            hdfsHelper.getFileSystem(defaultFS, configuration);
            this.path = new Path(buildFilePath(this.configuration.getString(Key.PATH)));
            this.mode = this.configuration.getString(Key.WRITE_MODE);
            this.tmpDirPath = new Path(buildTmpFilePath(this.configuration.getString(Key.PATH)));
            this.configuration.set(Key.PATH, this.path.toString());
            this.configuration.set(TMP_DIR, this.tmpDirPath.toString());
        }

        @Override
        public void prepare() {
            if (DELETE_IF_EXIST.equals(mode)) {
                LOGGER.info("writeMode [{}] . delete path if path is exist {}", mode, path);
                hdfsHelper.deleteDir(path);
            }
            if (!hdfsHelper.isPathExists(path.toString())) {
                hdfsHelper.mkdirs(path);
            } else if (hdfsHelper.isFile(path.toString())) {
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您填写的目标路径 [%s] 不能是文件", path.toString()));
            }
        }

        @Override
        public void post() {
            Path[] tmpFiles = hdfsHelper.hdfsDirList(this.tmpDirPath.toString());
            List<String> targetPathSet = new LinkedList<String>();
            List<String> tmpPathSet = new LinkedList<String>();
            for (Path tmpFile : tmpFiles) {
                String tmpFilePath = tmpFile.toUri().getPath();
                tmpPathSet.add(tmpFilePath);
                targetPathSet.add(this.path + removePrefix(tmpFilePath, this.tmpDirPath.toString()));
            }
            this.hdfsHelper.renameFile(tmpPathSet, targetPathSet);
        }

        @Override
        public void destroy() {
            // 清除中间目录
            this.hdfsHelper.deleteDir(this.tmpDirPath);
            this.hdfsHelper.closeFileSystem();
        }

        private void validate() {
            //path
            String pathString = this.configuration.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE);
            if (!pathString.startsWith("/")) {
                String message = String.format("请检查参数path:[%s],需要配置为绝对路径", path);
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE, message);
            } else if (pathString.contains("*") || pathString.contains("?")) {
                String message = String.format("请检查参数path:[%s],不能包含*,?等特殊字符", path);
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE, message);
            }
            // check mode
            String mode = this.configuration.getNecessaryValue(Key.WRITE_MODE, HdfsWriterErrorCode.REQUIRED_VALUE);
            mode = mode.toLowerCase().trim();
            if (!OVER_WRITE.equals(mode) && !UPDATE.equals(mode) && !DELETE_IF_EXIST.equals(mode)) {
                throw DataXException.asDataXException(HdfsWriterErrorCode.CONFIG_INVALID_EXCEPTION, Key.WRITE_MODE + "参数配置错误");
            } else {
                this.configuration.set(Key.WRITE_MODE, mode);
            }
            //check Kerberos
            Boolean haveKerberos = this.configuration.getBool(Key.HAVE_KERBEROS, false);
            if (haveKerberos) {
                this.configuration.getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, HdfsWriterErrorCode.REQUIRED_VALUE);
                this.configuration.getNecessaryValue(Key.KERBEROS_PRINCIPAL, HdfsWriterErrorCode.REQUIRED_VALUE);
            }
            if (!startWithSeparator(this.configuration.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE))) {
                throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR,
                        String.format("输出路径[%s]请使用绝对路径", this.configuration.getString(Key.PATH)));
            }
        }

        private String buildFilePath(String path) {
            String pathResult = path;
            boolean isEndWithSeparator = endWithSeparator(path);
            if (!isEndWithSeparator) {
                pathResult = path + Path.SEPARATOR;
            }
            return new Path(pathResult).toString();
        }

        /**
         * 创建临时目录
         *
         * @param userPath
         * @return
         */
        private String buildTmpFilePath(String userPath) {
            String tmpFilePath;
            boolean isEndWithSeparator = endWithSeparator(userPath);
            tmpFilePath = getTmpFilePath(isEndWithSeparator, userPath);
            while (hdfsHelper.isPathExists(tmpFilePath)) {
                tmpFilePath = getTmpFilePath(isEndWithSeparator, userPath);
            }
            return new Path(tmpFilePath + Path.SEPARATOR).toString();
        }

        private static final String getTmpFilePath(boolean isEndWithSeparator, String userPath) {
            String tmpSuffix = UUID.randomUUID().toString().replace('-', '_');
            String tmpFilePath;
            if (!isEndWithSeparator) {
                tmpFilePath = String.format("%s/_fscp_%s%s", userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
            } else if ("/".equals(userPath)) {
                tmpFilePath = String.format("%s/_fscp_%s%s", userPath, tmpSuffix, IOUtils.DIR_SEPARATOR);
            } else {
                tmpFilePath = String.format("%s/_fscp_%s%s", userPath.substring(0, userPath.length() - 1), tmpSuffix, IOUtils.DIR_SEPARATOR);
            }
            return tmpFilePath;
        }

        private static boolean endWithSeparator(String path) {
            boolean isEndWithSeparator = false;
            switch (IOUtils.DIR_SEPARATOR) {
                case IOUtils.DIR_SEPARATOR_UNIX:
                    isEndWithSeparator = path.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR_UNIX));
                    break;
                case IOUtils.DIR_SEPARATOR_WINDOWS:
                    isEndWithSeparator = path.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
                    break;
                default:
                    break;
            }
            return isEndWithSeparator;
        }

        private static boolean startWithSeparator(String path) {
            return path.startsWith(String
                    .valueOf(IOUtils.DIR_SEPARATOR_UNIX)) || path.startsWith(String.valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);
        private HdfsHelper hdfsHelper = null;
        private Configuration writerSliceConfig;

        private String defaultFS;
        private String path;
        private String mode;
        private String tmpFilePath;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.defaultFS = this.writerSliceConfig.getString(Key.DEFAULT_FS);
            this.path = this.writerSliceConfig.getString(Key.PATH);
            this.mode = this.writerSliceConfig.getString(Key.WRITE_MODE);
            hdfsHelper = new HdfsHelper();
            hdfsHelper.getFileSystem(defaultFS, writerSliceConfig);
            tmpFilePath = this.writerSliceConfig.getString(TMP_DIR);
        }


        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                try {
                    String dirPath = Record.RecordHelp.getPathString(record.getDirPath(), Path.SEPARATOR);
                    Path targetPath = new Path(this.path + Path.SEPARATOR + dirPath + Path.SEPARATOR + record.getFileName());
                    Path tmpPath = new Path(this.tmpFilePath + Path.SEPARATOR + dirPath + Path.SEPARATOR + record.getFileName());
                    Path parentPath = tmpPath.getParent();
                    boolean write = !UPDATE.equals(mode) || !this.hdfsHelper.isPathExists(targetPath.toString())
                            || (UPDATE.equals(mode) && record.getFileLength() != null && !record.getFileLength().equals(hdfsHelper.getFileLength(targetPath)));
                    if (write) {
                        if (!hdfsHelper.isPathExists(parentPath.toString())) {
                            try {
                                hdfsHelper.mkdirs(parentPath);
                            } catch (Exception e) {
                                // 防止多Task中创建同一个目录
                            }
                        }
                        // write to hdfs
                        this.hdfsHelper.writeToHdfs(record, this.writerSliceConfig, tmpPath.toString(), this.getTaskPluginCollector());
                    } else {
                        LOGGER.info("跳过写入文件 {}， 因为文件已经存在且长度一致", targetPath.toString());
                    }
                } finally {
                    record.destroy();
                }
            }
        }

        @Override
        public void destroy() {
            // 注意：不能在此处关闭fileSystem.因为和Job中使用的是同一个，job中还需要使用fileSystem
        }

        @Override
        public boolean supportFailOver() {
            return true;
        }
    }

    /**
     * @param value  原来的值
     * @param prefix 需要移除的前缀
     * @return
     */
    public static String removePrefix(String value, String prefix) {
        if (value.startsWith(prefix)) {
            return value.substring(prefix.length());
        }
        throw DataXException.asDataXException(HdfsWriterErrorCode.WRITER_RUNTIME_EXCEPTION, "输出路径请使用绝对路径");
    }
}
