package com.alibaba.datax.common.spi;

import com.alibaba.datax.common.base.BaseObject;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;

import java.util.List;

/**
 * 每个Writer插件需要实现Writer类，并在其内部实现Job、Task两个内部类。
 */
public abstract class Writer extends BaseObject {
    /**
     * 每个Writer插件必须实现Job内部类
     */
    public abstract static class Job extends AbstractJobPlugin {
        /**
         * 切分任务。<br>
         *
         * @param mandatoryNumber 为了做到Reader、Writer任务数对等，这里要求Writer插件必须按照源端的切分数进行切分。否则框架报错！
         */
        public abstract List<Configuration> split(int mandatoryNumber);
    }

    /**
     * 每个Writer插件必须实现Task内部类
     */
    public abstract static class Task extends AbstractTaskPlugin {

        /**
         * 开始写入
         *
         * @param lineReceiver
         */
        public abstract void startWrite(RecordReceiver lineReceiver);

        /**
         * 是否支持重试。重试时，对应的Reader也会重新执行
         *
         * @return
         */
        public boolean supportFailOver() {
            return false;
        }
    }
}
