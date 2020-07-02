package com.netease.ds.fscp.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wuqi3@corp.netease.com
 * @created 2020-07-02 15:31
 * @since
 */
public class LocalFileWriter extends Writer {

    public static class Job extends Writer.Job {

        private Configuration configuration;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            System.out.println("writer split");
            List<Configuration> configurations = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(this.configuration.clone());
            }
            return configurations;
        }

        @Override
        public void init() {
            System.out.println("writer init");
            this.configuration = super.getPluginJobConf();
            String src = this.getPluginJobConf().getString("src");
            File file = new File(src);
            if (!file.exists()) {
                file.mkdirs();
            }
        }

        @Override
        public void destroy() {
            System.out.println("writer destroy");
        }
    }

    public static class Task extends Writer.Task {

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            String src = this.getPluginJobConf().getString("src");
            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {
                System.out.println("writer task start: " + record.getDirPath() + "\\" + record.getFileName());
                String dirPath = src + "/" + record.getDirPath();
                File file = new File(dirPath);
                if (!file.exists()) {
                    file.mkdirs();
                }
                try {
                    FileUtils.copyInputStreamToFile(record.getInputStream(), new File(src + "/" + record.getDirPath() + "/" + record.getFileName()));
                } catch (Exception e) {
                    this.getTaskPluginCollector().collectDirtyRecord(record, e);
                } finally {
                    IOUtils.closeQuietly(record.getInputStream());
                }
            }
        }

        @Override
        public void init() {
            System.out.println("writer task init");
        }

        @Override
        public void destroy() {
            System.out.println("writer task destroy");
        }
    }
}
