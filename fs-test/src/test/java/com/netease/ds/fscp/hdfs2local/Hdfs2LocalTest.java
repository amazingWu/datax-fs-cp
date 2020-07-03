package com.netease.ds.fscp.hdfs2local;

import com.alibaba.datax.core.Engine;
import org.junit.Test;

/**
 * @author wuqi3@corp.netease.com
 * @created 2020-07-02 15:32
 * @since
 */
public class Hdfs2LocalTest {
    private String jobPath = "src/test/java/com/netease/ds/fscp/hdfs2Local/test-job.json";
    private String mammutJobPath = "src/test/java/com/netease/ds/fscp/hdfs2Local/test-mammut-job.json";
    private String jobId = "1";
    private String jobMode = "standalone";

    @Test
    public void test() throws Throwable {
        System.setProperty("datax.home", "src/test/resources/hdfs-2-local");
        Engine.entry(new String[]{
                "-job", jobPath, "-jobid", jobId, "-mode", jobMode
        });
    }

    @Test
    public void testMammut() throws Throwable {
        System.setProperty("java.security.krb5.conf", "src/test/resources/krb5.conf");
        System.setProperty("datax.home", "src/test/resources/hdfs-2-local");
        Engine.entry(new String[]{
                "-job", mammutJobPath, "-jobid", jobId, "-mode", jobMode
        });
    }
}
