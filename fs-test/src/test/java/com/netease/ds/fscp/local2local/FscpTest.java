package com.netease.ds.fscp.local2local;

import com.alibaba.datax.core.Engine;
import org.junit.Test;

/**
 * @author wuqi3@corp.netease.com
 * @created 2020-07-02 15:32
 * @since
 */
public class FscpTest {
    private String jobPath = "src/test/java/com/netease/ds/fscp/local2local/test-job.json";
    private String jobId = "1";
    private String jobMode = "standalone";

    @Test
    public void testJob() throws Throwable {
        System.setProperty("datax.home", "src/test/resources/local-2-local");
        Engine.entry(new String[]{
                "-job", jobPath, "-jobid", jobId, "-mode", jobMode
        });
    }
}
