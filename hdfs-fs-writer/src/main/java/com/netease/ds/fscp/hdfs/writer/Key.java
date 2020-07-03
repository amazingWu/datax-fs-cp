package com.netease.ds.fscp.hdfs.writer;

/**
 * Created by shf on 15/10/8.
 */
public class Key {
    // must have
    public static final String PATH = "path";
    public final static String DEFAULT_FS = "defaultFS";
    public static final String WRITE_MODE = "writeMode";

    // optional
    public static final String APPEND = "append";
    // Kerberos
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";
    // hadoop config
    public static final String HADOOP_CONFIG = "hadoopConfig";
}
