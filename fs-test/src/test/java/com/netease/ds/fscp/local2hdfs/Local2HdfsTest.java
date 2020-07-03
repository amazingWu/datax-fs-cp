package com.netease.ds.fscp.local2hdfs;

import com.alibaba.datax.core.Engine;
import org.junit.Test;

/**
 * @author wuqi3@corp.netease.com
 * @created 2020-07-02 15:32
 * @since
 */
public class Local2HdfsTest {
    private String jobPath = "src/test/java/com/netease/ds/fscp/local2Hdfs/test-job.json";
    private String jobId = "1";
    private String jobMode = "standalone";

    @Test
    public void test() throws Throwable {
        System.setProperty("datax.home", "src/test/resources/local-2-hdfs");
        Engine.entry(new String[]{
                "-job", jobPath, "-jobid", jobId, "-mode", jobMode
        });
    }
}
