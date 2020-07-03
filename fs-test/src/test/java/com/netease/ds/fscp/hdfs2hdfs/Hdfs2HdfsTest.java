package com.netease.ds.fscp.hdfs2hdfs;

import com.alibaba.datax.core.Engine;
import org.junit.Test;

/**
 * @author wuqi3@corp.netease.com
 * @created 2020-07-02 15:32
 * @since
 */
public class Hdfs2HdfsTest {
    private String jobPath = "src/test/java/com/netease/ds/fscp/hdfs2Hdfs/test-job.json";
    private String updateJobPath = "src/test/java/com/netease/ds/fscp/hdfs2Hdfs/test-job-update.json";
    private String mammut2mammutJobPath = "src/test/java/com/netease/ds/fscp/hdfs2Hdfs/test-mammut2mammut-job.json";
    private String jobId = "1";
    private String jobMode = "standalone";

    @Test
    public void test() throws Throwable {
        System.setProperty("datax.home", "src/test/resources/hdfs-2-hdfs");
        Engine.entry(new String[]{
                "-job", jobPath, "-jobid", jobId, "-mode", jobMode
        });
    }


    @Test
    public void testUpdate() throws Throwable {
        System.setProperty("datax.home", "src/test/resources/hdfs-2-hdfs");
        Engine.entry(new String[]{
                "-job", updateJobPath, "-jobid", jobId, "-mode", jobMode
        });
    }
}
