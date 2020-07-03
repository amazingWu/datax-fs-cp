package com.netease.ds.fscp.hdfs.reader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

/**
 * Created by mingya.wmy on 2015/8/12.
 */
public class DFSUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DFSUtil.class);
    public FileSystem fileSystem = null;
    public JobConf conf = null;
    public org.apache.hadoop.conf.Configuration hadoopConf = null;
    public static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";
    public static final String HDFS_DEFAULTFS_KEY = "fs.defaultFS";

    // Kerberos
    private Boolean haveKerberos = false;
    private String kerberosKeytabFilePath;
    private String kerberosPrincipal;

    public void getFileSystem(String defaultFS, Configuration taskConfig) {
        hadoopConf = new org.apache.hadoop.conf.Configuration();

        Configuration hadoopSiteParams = taskConfig.getConfiguration(Key.HADOOP_CONFIG);
        JSONObject hadoopSiteParamsAsJsonObject = JSON.parseObject(taskConfig.getString(Key.HADOOP_CONFIG));
        if (null != hadoopSiteParams) {
            Set<String> paramKeys = hadoopSiteParams.getKeys();
            for (String each : paramKeys) {
                hadoopConf.set(each, hadoopSiteParamsAsJsonObject.getString(each));
            }
        }
        hadoopConf.set(HDFS_DEFAULTFS_KEY, defaultFS);

        //是否有Kerberos认证
        this.haveKerberos = taskConfig.getBool(Key.HAVE_KERBEROS, false);
        if (haveKerberos) {
            this.kerberosKeytabFilePath = taskConfig.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
            this.kerberosPrincipal = taskConfig.getString(Key.KERBEROS_PRINCIPAL);
            hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
        }
        this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);
        conf = new JobConf(hadoopConf);
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            String message = String.format("获取FileSystem时发生网络IO异常,请检查您的网络是否正常!HDFS地址：[%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.CONNECT_HDFS_IO_ERROR, e);
        } catch (Exception e) {
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }

        if (null == fileSystem || null == conf) {
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }
    }

    private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath) {
        if (haveKerberos && StringUtils.isNotBlank(this.kerberosPrincipal) && StringUtils.isNotBlank(this.kerberosKeytabFilePath)) {
            UserGroupInformation.setConfiguration(this.hadoopConf);
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
            } catch (Exception e) {
                String message = String.format("kerberos认证失败,请确定kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]填写正确",
                        kerberosKeytabFilePath, kerberosPrincipal);
                LOG.error(message);
                throw DataXException.asDataXException(HdfsReaderErrorCode.KERBEROS_LOGIN_ERROR, e);
            }
        }
    }

    public List<Reader.FileEntity> getHDFSAllFiles(String hdfsPath) {
        List<Reader.FileEntity> result = Lists.newLinkedList();
        try {
            //判断hdfsPath是否包含正则符号
            if (hdfsPath.contains("*") || hdfsPath.contains("?")) {
                Path path = new Path(hdfsPath);
                FileStatus stats[] = this.fileSystem.globStatus(path);
                for (FileStatus f : stats) {
                    if (f.isFile()) {
                        Reader.FileEntity fileEntity = new Reader.FileEntity();
                        fileEntity.setFullPath(f.getPath().toString());
                        fileEntity.setDirRelativePath(null);
                        fileEntity.setFileName(f.getPath().getName());
                        fileEntity.setFileLength(f.getLen());
                        result.add(fileEntity);
                    } else if (f.isDirectory()) {
                        getHDFSAllFilesNORegex(f.getPath().toString(), result, Lists.<String>newLinkedList());
                    }
                }
            } else {
                getHDFSAllFilesNORegex(hdfsPath, result, Lists.<String>newLinkedList());
            }
            return result;
        } catch (IOException e) {
            String message = String.format("无法读取路径[%s]下的所有文件,请确认您的配置项fs.defaultFS, path的值是否正确，" +
                    "是否有读写权限，网络是否已断开！", hdfsPath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.PATH_CONFIG_ERROR, e);
        }
    }

    private void getHDFSAllFilesNORegex(String path, List<Reader.FileEntity> result, List<String> relativeDirPath) throws IOException {
        // 获取要读取的文件的根目录
        Path listFiles = new Path(path);
        // If the network disconnected, this method will retry 45 times
        // each time the retry interval for 20 seconds
        // 获取要读取的文件的根目录的所有二级子文件目录
        FileStatus stats[] = this.fileSystem.listStatus(listFiles);
        for (FileStatus f : stats) {
            // 判断是不是目录，如果是目录，递归调用
            if (f.isDirectory()) {
                LOG.info(String.format("[%s] 是目录, 递归获取该目录下的文件", f.getPath().toString()));
                getHDFSAllFilesNORegex(f.getPath().toString(), result, copyAndAdd(relativeDirPath, f.getPath().getName()));
            } else if (f.isFile()) {
                Reader.FileEntity fileEntity = new Reader.FileEntity();
                fileEntity.setFullPath(f.getPath().toString());
                fileEntity.setDirRelativePath(relativeDirPath);
                fileEntity.setFileName(f.getPath().getName());
                fileEntity.setFileLength(f.getLen());
                result.add(fileEntity);
            } else {
                String message = String.format("该路径[%s]文件类型既不是目录也不是文件，插件自动忽略。",
                        f.getPath().toString());
                LOG.info(message);
            }
        }
    }

    private static final List<String> copyAndAdd(List<String> list, String value) {
        List<String> result = Lists.newLinkedList(list);
        result.add(value);
        return result;
    }

    public InputStream getInputStream(String filePath) {
        InputStream inputStream;
        Path path = new Path(filePath);
        try {
            //If the network disconnected, this method will retry 45 times
            //each time the retry interval for 20 seconds
            inputStream = this.fileSystem.open(path);
            return inputStream;
        } catch (IOException e) {
            String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message, e);
        }
    }

    public void closeFileSystem() {
        try {
            fileSystem.close();
        } catch (IOException e) {
            String message = String.format("关闭FileSystem时发生IO异常,请检查您的网络是否正常！");
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.CONNECT_HDFS_IO_ERROR, e);
        }
    }

}
