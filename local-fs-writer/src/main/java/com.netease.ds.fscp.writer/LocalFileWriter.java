package com.netease.ds.fscp.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wuqi3@corp.netease.com
 * @created 2020-07-02 15:31
 * @since
 */
public class LocalFileWriter extends Writer {
    private static final String OVER_WRITE = "overwrite";
    private static final String UPDATE = "update";


    public static class Job extends Writer.Job {
        private Configuration configuration;

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
            String src = this.getPluginJobConf().getString(Key.PATH);
            File file = new File(src);
            if (!file.exists()) {
                file.mkdirs();
            }
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            String src = this.getPluginJobConf().getString(Key.PATH);
            // 可选有update和overwrite
            String mode = this.getPluginJobConf().getString(Key.WRITE_MODE, OVER_WRITE);
            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                String dirPath = src + File.separator + Record.RecordHelp.getPathString(record.getDirPath(), File.separator);
                File file = new File(dirPath);
                if (!file.exists()) {
                    file.mkdirs();
                }
                try {
                    File targetFile = new File(dirPath + File.separator + record.getFileName());
                    if (UPDATE.equals(mode) && record.getFileLength() != null && targetFile.exists() && targetFile.length() == record.getFileLength()) {
                        // 长度一致，则认为文件已经存在
                        LOGGER.debug(String.format("%s 已经存在", targetFile.getAbsolutePath()));
                        return;
                    }
                    FileUtils.copyInputStreamToFile(record.getInputStream(), targetFile);
                    if (record.getFileMd5() != null) {
                        FileInputStream fileInputStream = new FileInputStream(targetFile);
                        if (!DigestUtils.md5Hex(fileInputStream).equals(record.getFileMd5())) {
                            throw DataXException.asDataXException(CommonErrorCode.RUNTIME_ERROR, targetFile.getAbsolutePath() + " md5不一致");
                        }
                    }
                } catch (Exception e) {
                    throw DataXException.asDataXException(CommonErrorCode.RUNTIME_ERROR, e.getMessage(), e);
                } finally {
                    record.destroy();
                }
            }
        }

        @Override
        public void init() {
        }

        @Override
        public void destroy() {
        }

        @Override
        public boolean supportFailOver() {
            return true;
        }
    }
}
